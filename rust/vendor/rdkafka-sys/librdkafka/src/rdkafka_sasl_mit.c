/*
 * librdkafka - The Apache Kafka C/C++ library
 *
 * SASL GSSAPI provider backed by MIT Kerberos (gssapi64.dll / gssapi32.dll),
 * loaded dynamically at runtime.
 *
 * Why this exists: on Windows librdkafka only ships the Win32 SSPI provider,
 * which normalises the SPN hostname to upper case before asking the KDC. MIT
 * KDC SPNs are case sensitive, so a broker registered as
 * "kafka/bd-pr-rn1@REALM" can never be found by SSPI (Target unknown).
 * This provider talks to MIT Kerberos directly, keeps the hostname as given
 * and reads the usual MIT credential cache (KRB5CCNAME / KRB5_CONFIG).
 *
 * Enable it by setting the environment variable RDKAFKA_SASL_MIT=1 (any
 * non-empty value). The provider is selected ahead of SSPI for GSSAPI.
 */

#include "rdkafka_int.h"
#include "rdkafka_transport.h"
#include "rdkafka_transport_int.h"
#include "rdkafka_sasl.h"
#include "rdkafka_sasl_int.h"

#ifdef _WIN32

#include <windows.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**
 * @brief Copy a process environment variable into the CRT's environment.
 *
 * Rust's std::env::set_var updates the Win32 process environment block, while
 * getenv() reads the CRT snapshot taken at startup. Kerberos libraries loaded
 * later read getenv(), so sync the values we care about into the CRT.
 */
static void rd_kafka_sasl_mit_sync_env(const char *name) {
        char val[1024];
        DWORD n = GetEnvironmentVariableA(name, val, (DWORD)sizeof(val));
        if (n > 0 && n < sizeof(val))
                _putenv_s(name, val);
}

typedef uint32_t rd_mit_uint32_t;

typedef struct rd_mit_buffer_s {
        size_t length;
        void *value;
} rd_mit_buffer_t;

typedef struct rd_mit_name_s *rd_mit_name_t;
typedef struct rd_mit_ctx_s *rd_mit_ctx_t;

typedef struct rd_mit_oid_s {
        rd_mit_uint32_t length;
        void *elements;
} rd_mit_oid_t;

/* gss_init_sec_context 参数较多，按 MIT GSSAPI ABI 声明 */
typedef rd_mit_uint32_t (*pfn_gss_import_name)(rd_mit_uint32_t *,
                                               rd_mit_buffer_t *,
                                               rd_mit_oid_t *,
                                               rd_mit_name_t *);
typedef rd_mit_uint32_t (*pfn_gss_init_sec_context)(
    rd_mit_uint32_t *,
    void *,
    rd_mit_ctx_t *,
    rd_mit_name_t,
    rd_mit_oid_t *,
    rd_mit_uint32_t,
    rd_mit_uint32_t,
    void *,
    void *,
    rd_mit_oid_t **,
    rd_mit_buffer_t *,
    rd_mit_uint32_t *,
    rd_mit_uint32_t *);
typedef rd_mit_uint32_t (*pfn_gss_wrap)(rd_mit_uint32_t *,
                                        rd_mit_ctx_t,
                                        int,
                                        rd_mit_uint32_t,
                                        rd_mit_buffer_t *,
                                        int *,
                                        rd_mit_buffer_t *);
typedef rd_mit_uint32_t (*pfn_gss_unwrap)(rd_mit_uint32_t *,
                                          rd_mit_ctx_t,
                                          rd_mit_buffer_t *,
                                          rd_mit_buffer_t *,
                                          int *,
                                          rd_mit_uint32_t *);
typedef rd_mit_uint32_t (*pfn_gss_release_buffer)(rd_mit_uint32_t *,
                                                  rd_mit_buffer_t *);
typedef rd_mit_uint32_t (*pfn_gss_release_name)(rd_mit_uint32_t *,
                                                rd_mit_name_t *);
typedef rd_mit_uint32_t (*pfn_gss_delete_sec_context)(rd_mit_uint32_t *,
                                                      rd_mit_ctx_t *,
                                                      rd_mit_buffer_t *);
typedef rd_mit_uint32_t (*pfn_gss_display_status)(rd_mit_uint32_t *,
                                                  rd_mit_uint32_t,
                                                  int,
                                                  rd_mit_oid_t *,
                                                  rd_mit_uint32_t *,
                                                  rd_mit_buffer_t *);

#define RD_MIT_GSS_S_COMPLETE 0
#define RD_MIT_GSS_S_CONTINUE_NEEDED 1

/* GSS_C_NT_HOSTBASED_SERVICE = 1.2.840.113554.1.2.1.4 */
static unsigned char rd_mit_nt_hostbased_bytes[] = {
    0x2a, 0x86, 0x48, 0x86, 0xf7, 0x12, 0x01, 0x02, 0x01, 0x04};

static struct {
        int loaded;
        int ok;
        HMODULE lib;
        pfn_gss_import_name import_name;
        pfn_gss_init_sec_context init_sec_context;
        pfn_gss_wrap wrap;
        pfn_gss_unwrap unwrap;
        pfn_gss_release_buffer release_buffer;
        pfn_gss_release_name release_name;
        pfn_gss_delete_sec_context delete_sec_context;
        pfn_gss_display_status display_status;
} rd_mit_gss;

/**
 * @brief Load MIT gssapi from the usual MIT Kerberos for Windows locations.
 * @returns 1 on success.
 */
static int rd_kafka_sasl_mit_gss_load(void) {
        static const char *paths[] = {
            "gssapi64.dll",
            "D:\\MIT\\Kerberos\\bin\\gssapi64.dll",
            "C:\\Program Files\\MIT\\Kerberos\\bin\\gssapi64.dll",
            "gssapi32.dll",
        };
        /* 0 = 未加载, 1 = 正在加载, 2 = 成功, -1 = 失败。
         * rdkafka 的多个客户端线程可能并发首次连接，必须互斥。 */
        static volatile LONG state = 0;
        size_t i;
        LONG cur;

        cur = InterlockedCompareExchange(&state, 0, 0);
        if (cur == 2)
                return 1;
        if (cur == -1)
                return 0;
        if (cur == 1) {
                while ((cur = InterlockedCompareExchange(&state, 1, 1)) == 1)
                        Sleep(1);
                return cur == 2;
        }

        if (InterlockedCompareExchange(&state, 1, 0) != 0) {
                while ((cur = InterlockedCompareExchange(&state, 1, 1)) == 1)
                        Sleep(1);
                return cur == 2;
        }

        /* gssapi64.dll 依赖同目录的 krb5_64.dll 等，先把 MIT 目录加入搜索路径 */
        SetDllDirectoryA("D:\\MIT\\Kerberos\\bin");

        /* Kerberos 库用 getenv() 读这些变量，先同步进 CRT 环境 */
        rd_kafka_sasl_mit_sync_env("KRB5_CONFIG");
        rd_kafka_sasl_mit_sync_env("KRB5CCNAME");
        rd_kafka_sasl_mit_sync_env("KRB5_KTNAME");

        for (i = 0; i < RD_ARRAYSIZE(paths); i++) {
                rd_mit_gss.lib = LoadLibraryA(paths[i]);
                if (rd_mit_gss.lib)
                        break;
        }
        if (!rd_mit_gss.lib) {
                InterlockedExchange(&state, -1);
                return 0;
        }

        rd_mit_gss.import_name = (pfn_gss_import_name)(void *)GetProcAddress(
            rd_mit_gss.lib, "gss_import_name");
        rd_mit_gss.init_sec_context =
            (pfn_gss_init_sec_context)(void *)GetProcAddress(
                rd_mit_gss.lib, "gss_init_sec_context");
        rd_mit_gss.wrap =
            (pfn_gss_wrap)(void *)GetProcAddress(rd_mit_gss.lib, "gss_wrap");
        rd_mit_gss.unwrap =
            (pfn_gss_unwrap)(void *)GetProcAddress(rd_mit_gss.lib, "gss_unwrap");
        rd_mit_gss.release_buffer =
            (pfn_gss_release_buffer)(void *)GetProcAddress(
                rd_mit_gss.lib, "gss_release_buffer");
        rd_mit_gss.release_name =
            (pfn_gss_release_name)(void *)GetProcAddress(rd_mit_gss.lib,
                                                         "gss_release_name");
        rd_mit_gss.delete_sec_context =
            (pfn_gss_delete_sec_context)(void *)GetProcAddress(
                rd_mit_gss.lib, "gss_delete_sec_context");
        rd_mit_gss.display_status =
            (pfn_gss_display_status)(void *)GetProcAddress(
                rd_mit_gss.lib, "gss_display_status");

        rd_mit_gss.ok = rd_mit_gss.import_name && rd_mit_gss.init_sec_context &&
                        rd_mit_gss.wrap && rd_mit_gss.unwrap &&
                        rd_mit_gss.release_buffer;
        InterlockedExchange(&state, rd_mit_gss.ok ? 2 : -1);
        return rd_mit_gss.ok;
}

/**
 * @brief Format a GSSAPI minor status into errstr.
 */
static void rd_kafka_sasl_mit_minor2str(rd_mit_uint32_t minor,
                                        char *errstr,
                                        size_t errstr_size) {
        rd_mit_buffer_t msg = {0, NULL};
        rd_mit_uint32_t msgctx = 0, m2 = 0;
        size_t used = 0;

        if (!rd_mit_gss.display_status)
                return;

        while (rd_mit_gss.display_status(&m2, minor, 1, NULL, &msgctx, &msg) ==
                   0 &&
               msg.length) {
                int n = rd_snprintf(errstr + used, errstr_size - used,
                                    "%s%.*s", used ? "; " : "", (int)msg.length,
                                    (char *)msg.value);
                if (n > 0)
                        used += (size_t)n;
                rd_mit_gss.release_buffer(&m2, &msg);
                if (!msgctx || used + 4 >= errstr_size)
                        break;
        }
}

typedef enum {
        RD_KAFKA_SASL_MIT_STAGE_CONTEXT = 0,
        RD_KAFKA_SASL_MIT_STAGE_SECLAYER,
        RD_KAFKA_SASL_MIT_STAGE_DONE
} rd_kafka_sasl_mit_stage_t;

typedef struct rd_kafka_sasl_mit_state_s {
        rd_mit_ctx_t ctx;
        rd_mit_name_t target;
        rd_kafka_sasl_mit_stage_t stage;
} rd_kafka_sasl_mit_state_t;

static void rd_kafka_sasl_mit_state_destroy(rd_kafka_sasl_mit_state_t *state) {
        rd_mit_uint32_t minor;
        rd_mit_buffer_t out = {0, NULL};

        if (state->ctx && rd_mit_gss.delete_sec_context)
                rd_mit_gss.delete_sec_context(&minor, &state->ctx, &out);
        if (out.value && rd_mit_gss.release_buffer)
                rd_mit_gss.release_buffer(&minor, &out);
        if (state->target && rd_mit_gss.release_name)
                rd_mit_gss.release_name(&minor, &state->target);
}

/**
 * @brief Answer the RFC 4752 security-layer negotiation.
 *
 * Kafka's broker sends its layer offer as a GSSAPI wrap token (not an AP-REP),
 * so the client unwraps it and echoes the offer back wrapped with "no security
 * layer" selected. This mirrors the working Go client (franz-go
 * pkg/sasl/kerberos).
 */
static int rd_kafka_sasl_mit_reply_seclayer(rd_kafka_transport_t *rktrans,
                                            const void *token,
                                            size_t tokenlen,
                                            char *errstr,
                                            size_t errstr_size) {
        rd_kafka_sasl_mit_state_t *state = rktrans->rktrans_sasl.state;
        rd_mit_buffer_t in    = {tokenlen, (void *)token};
        rd_mit_buffer_t plain = {0, NULL};
        rd_mit_buffer_t out   = {0, NULL};
        rd_mit_uint32_t major, minor;
        int conf_state = 0;
        int qop        = 0;
        int r;

        major = rd_mit_gss.unwrap(&minor, state->ctx, &in, &plain, &conf_state,
                                  &qop);
        if (major != RD_MIT_GSS_S_COMPLETE) {
                char minorstr[256] = "";
                rd_kafka_sasl_mit_minor2str(minor, minorstr, sizeof(minorstr));
                rd_snprintf(errstr, errstr_size,
                            "SASL GSSAPI (MIT): gss_unwrap of the broker layer "
                            "offer failed: major=0x%x minor=0x%x %s",
                            major, minor, minorstr);
                return -1;
        }

        if (plain.length >= 1 && plain.value) {
                unsigned char layers = *(unsigned char *)plain.value;
                if (!(layers & 0x01)) {
                        rd_snprintf(errstr, errstr_size,
                                    "SASL GSSAPI (MIT): broker requires a "
                                    "security layer (0x%02x), only \"no layer\" "
                                    "is supported",
                                    layers);
                        rd_mit_gss.release_buffer(&minor, &plain);
                        return -1;
                }
        }

        major = rd_mit_gss.wrap(&minor, state->ctx, 0 /* conf_req */, 0, &plain,
                                &conf_state, &out);
        if (plain.value)
                rd_mit_gss.release_buffer(&minor, &plain);
        if (major != RD_MIT_GSS_S_COMPLETE) {
                char minorstr[256] = "";
                rd_kafka_sasl_mit_minor2str(minor, minorstr, sizeof(minorstr));
                rd_snprintf(errstr, errstr_size,
                            "SASL GSSAPI (MIT): gss_wrap of the security layer "
                            "reply failed: major=0x%x minor=0x%x %s",
                            major, minor, minorstr);
                return -1;
        }

        r = rd_kafka_sasl_send(rktrans, out.value, (int)out.length, errstr,
                               errstr_size);
        if (out.value)
                rd_mit_gss.release_buffer(&minor, &out);

        state->stage = RD_KAFKA_SASL_MIT_STAGE_DONE;
        if (r != -1)
                rd_kafka_sasl_auth_done(rktrans);
        return r;
}

/**
 * @brief Run/continue the GSSAPI context establishment.
 *
 * The first call (client_new, no input) produces the AP-REQ. Kafka brokers do
 * not send an AP-REP back: their next token is the wrapped security-layer
 * offer, so the context is expected to complete in one step.
 */
static int rd_kafka_sasl_mit_context_step(rd_kafka_transport_t *rktrans,
                                          const void *inbuf,
                                          size_t insize,
                                          char *errstr,
                                          size_t errstr_size) {
        rd_kafka_sasl_mit_state_t *state = rktrans->rktrans_sasl.state;
        rd_mit_buffer_t in = {(size_t)insize, (void *)inbuf};
        rd_mit_buffer_t out = {0, NULL};
        rd_mit_uint32_t major, minor;
        rd_mit_uint32_t flags = 0;
        int r = 0;

        major = rd_mit_gss.init_sec_context(
            &minor, NULL, &state->ctx, state->target, NULL,
            (rd_mit_uint32_t)(0x0004 | 0x0008) /* REPLAY|SEQUENCE */, 0, NULL,
            (inbuf && insize) ? &in : NULL, NULL, &out, &flags, NULL);


        if (out.value && out.length > 0) {
                r = rd_kafka_sasl_send(rktrans, out.value, (int)out.length,
                                       errstr, errstr_size);
                rd_mit_gss.release_buffer(&minor, &out);
                if (r == -1)
                        return -1;
        }

        if (major == RD_MIT_GSS_S_CONTINUE_NEEDED)
                return 0;

        if (major != RD_MIT_GSS_S_COMPLETE) {
                char minorstr[256] = "";
                rd_kafka_sasl_mit_minor2str(minor, minorstr, sizeof(minorstr));
                rd_snprintf(errstr, errstr_size,
                            "SASL GSSAPI (MIT): gss_init_sec_context failed: "
                            "major=0x%x minor=0x%x %s",
                            major, minor, minorstr);
                return -1;
        }

        rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY, "SASLAUTH",
                   "SASL GSSAPI (MIT): context established");
        /* 上下文已建立：接下来等 broker 发来的安全层协商 token */
        state->stage = RD_KAFKA_SASL_MIT_STAGE_SECLAYER;
        return 0;
}

/**
 * @brief Provider: new connection, produce the initial GSSAPI token.
 */
static int rd_kafka_sasl_mit_client_new(rd_kafka_transport_t *rktrans,
                                        const char *hostname,
                                        char *errstr,
                                        size_t errstr_size) {
        rd_kafka_t *rk = rktrans->rktrans_rkb->rkb_rk;
        rd_kafka_sasl_mit_state_t *state;
        rd_mit_oid_t nt_hostbased;
        rd_mit_buffer_t nb;
        rd_mit_uint32_t major, minor;
        char spn[512];

        if (!rd_kafka_sasl_mit_gss_load()) {
                rd_snprintf(errstr, errstr_size,
                            "SASL GSSAPI (MIT): could not load gssapi64.dll / "
                            "gssapi32.dll (install MIT Kerberos for Windows)");
                return -1;
        }

        rd_snprintf(spn, sizeof(spn), "%s@%s", rk->rk_conf.sasl.service_name,
                    hostname);

        state                       = rd_calloc(1, sizeof(*state));
        rktrans->rktrans_sasl.state = state;

        nt_hostbased.length   = sizeof(rd_mit_nt_hostbased_bytes);
        nt_hostbased.elements = rd_mit_nt_hostbased_bytes;
        nb.length             = strlen(spn);
        nb.value              = spn;

        major = rd_mit_gss.import_name(&minor, &nb, &nt_hostbased,
                                       &state->target);
        if (major != RD_MIT_GSS_S_COMPLETE) {
                char minorstr[256] = "";
                rd_kafka_sasl_mit_minor2str(minor, minorstr, sizeof(minorstr));
                rd_snprintf(errstr, errstr_size,
                            "SASL GSSAPI (MIT): gss_import_name(\"%s\") failed: "
                            "major=0x%x minor=0x%x %s",
                            spn, major, minor, minorstr);
                rd_kafka_sasl_mit_state_destroy(state);
                rd_free(state);
                rktrans->rktrans_sasl.state = NULL;
                return -1;
        }

        rd_rkb_dbg(rktrans->rktrans_rkb, SECURITY, "SASLAUTH",
                   "SASL GSSAPI (MIT): authenticating to %s", spn);

        return rd_kafka_sasl_mit_context_step(rktrans, NULL, 0, errstr,
                                              errstr_size);
}

/**
 * @brief Provider: handle a SASL frame from the broker.
 */
static int rd_kafka_sasl_mit_recv(struct rd_kafka_transport_s *rktrans,
                                  const void *buf,
                                  size_t size,
                                  char *errstr,
                                  size_t errstr_size) {
        rd_kafka_sasl_mit_state_t *state = rktrans->rktrans_sasl.state;

        if (!state)
                return -1;

        switch (state->stage) {
        case RD_KAFKA_SASL_MIT_STAGE_CONTEXT:
                return rd_kafka_sasl_mit_context_step(rktrans, buf, size,
                                                      errstr, errstr_size);

        case RD_KAFKA_SASL_MIT_STAGE_SECLAYER: {
                if (size == 0) {
                        state->stage = RD_KAFKA_SASL_MIT_STAGE_DONE;
                        rd_kafka_sasl_auth_done(rktrans);
                        return 0;
                }
                return rd_kafka_sasl_mit_reply_seclayer(rktrans, buf, size,
                                                       errstr, errstr_size);
        }

        case RD_KAFKA_SASL_MIT_STAGE_DONE:
        default:
                if (size > 0) {
                        rd_snprintf(errstr, errstr_size,
                                    "SASL GSSAPI (MIT): unexpected post-auth "
                                    "frame (%" PRIusz " bytes)",
                                    size);
                        return -1;
                }
                return 0;
        }
}

/**
 * @brief Provider: decommission per-connection state.
 */
static void rd_kafka_sasl_mit_close(rd_kafka_transport_t *rktrans) {
        rd_kafka_sasl_mit_state_t *state = rktrans->rktrans_sasl.state;

        if (!state)
                return;

        rd_kafka_sasl_mit_state_destroy(state);
        rd_free(state);
        rktrans->rktrans_sasl.state = NULL;
}

static int rd_kafka_sasl_mit_conf_validate(rd_kafka_t *rk,
                                           char *errstr,
                                           size_t errstr_size) {
        if (!rk->rk_conf.sasl.service_name) {
                rd_snprintf(errstr, errstr_size,
                            "sasl.kerberos.service.name must be set");
                return -1;
        }
        return 0;
}

/**
 * @brief True when the MIT provider should be used instead of SSPI.
 *
 * Read through the Win32 API (not getenv) so a value set by the embedding
 * application at runtime is visible.
 */
rd_bool_t rd_kafka_sasl_mit_enabled(void) {
        char val[8];
        return GetEnvironmentVariableA("RDKAFKA_SASL_MIT", val, sizeof(val)) > 0
                   ? rd_true
                   : rd_false;
}

const struct rd_kafka_sasl_provider rd_kafka_sasl_mit_provider = {
    .name          = "MIT Kerberos GSSAPI",
    .client_new    = rd_kafka_sasl_mit_client_new,
    .recv          = rd_kafka_sasl_mit_recv,
    .close         = rd_kafka_sasl_mit_close,
    .conf_validate = rd_kafka_sasl_mit_conf_validate,
};

#endif /* _WIN32 */
