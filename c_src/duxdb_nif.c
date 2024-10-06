#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <erl_nif.h>
#include <duckdb.h>

static ERL_NIF_TERM am_ok;
static ERL_NIF_TERM am_error;
static ERL_NIF_TERM am_nil;
static ERL_NIF_TERM am_badarg;
static ERL_NIF_TERM am_system_limit;

static ErlNifResourceType *db_type = NULL;
static ErlNifResourceType *conn_type = NULL;
static ErlNifResourceType *config_type = NULL;

typedef struct db
{
    duckdb_database db;
} db_t;

typedef struct conn
{
    duckdb_connection conn;
} conn_t;

typedef struct config
{
    duckdb_config config;
} config_t;

static void
db_type_destructor(ErlNifEnv *env, void *arg)
{
    db_t *db = (db_t *)arg;
    if (db->db)
        duckdb_close(&db->db);
}

static void
conn_type_destructor(ErlNifEnv *env, void *arg)
{
    conn_t *conn = (conn_t *)arg;
    if (conn->conn)
        duckdb_disconnect(&conn->conn);
}

static void
config_type_destructor(ErlNifEnv *env, void *arg)
{
    config_t *config = (config_t *)arg;
    if (config->config)
        duckdb_destroy_config(&config->config);
}

static int
on_load(ErlNifEnv *env, void **priv, ERL_NIF_TERM info)
{
    am_ok = enif_make_atom(env, "ok");
    am_error = enif_make_atom(env, "error");
    am_nil = enif_make_atom(env, "nil");
    am_badarg = enif_make_atom(env, "badarg");
    am_system_limit = enif_make_atom(env, "system_limit");

    db_type = enif_open_resource_type(env, "duxdb", "db_type", db_type_destructor, ERL_NIF_RT_CREATE, NULL);
    if (!db_type)
        return -1;

    conn_type = enif_open_resource_type(env, "duxdb", "conn_type", conn_type_destructor, ERL_NIF_RT_CREATE, NULL);
    if (!conn_type)
        return -1;

    config_type = enif_open_resource_type(env, "duxdb", "config_type", config_type_destructor, ERL_NIF_RT_CREATE, NULL);
    if (!config_type)
        return -1;

    return 0;
}

static ERL_NIF_TERM
make_binary(ErlNifEnv *env, const char *bytes, size_t size)
{
    ERL_NIF_TERM bin;
    uint8_t *data = enif_make_new_binary(env, size, &bin);
    memcpy(data, bytes, size);
    return bin;
}

static ERL_NIF_TERM
make_badarg(ErlNifEnv *env, ERL_NIF_TERM arg)
{
    ERL_NIF_TERM badarg = enif_make_tuple2(env, am_badarg, arg);
    return enif_raise_exception(env, badarg);
}

static ERL_NIF_TERM
duxdb_library_version(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    const char *version = duckdb_library_version();
    return make_binary(env, version, strlen(version));
}

static ERL_NIF_TERM
duxdb_create_config(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    config_t *config = enif_alloc_resource(config_type, sizeof(config_t));
    if (!config)
        return enif_raise_exception(env, am_system_limit);

    if (duckdb_create_config(&config->config) == DuckDBError)
    {
        config->config = NULL;
        enif_release_resource(config);
        return enif_raise_exception(env, am_system_limit);
    }

    ERL_NIF_TERM result = enif_make_resource(env, config);
    enif_release_resource(config);
    return result;
}

static ERL_NIF_TERM
duxdb_config_count(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    size_t count = duckdb_config_count();
    return enif_make_uint64(env, count);
}

static ERL_NIF_TERM
duxdb_get_config_flag(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[0], &idx))
        return make_badarg(env, argv[0]);

    const char *name;
    const char *description;
    if (duckdb_get_config_flag(idx, &name, &description) == DuckDBError)
        return make_badarg(env, argv[0]);

    ERL_NIF_TERM bin_name = make_binary(env, name, strlen(name));
    ERL_NIF_TERM bin_description = make_binary(env, description, strlen(description));
    return enif_make_tuple2(env, bin_name, bin_description);
}

static ERL_NIF_TERM
duxdb_set_config(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    config_t *config;
    if (!enif_get_resource(env, argv[0], config_type, (void **)&config) || !config->config)
        return make_badarg(env, argv[0]);

    ErlNifBinary name;
    if (!enif_inspect_iolist_as_binary(env, argv[1], &name))
        return make_badarg(env, argv[1]);

    ErlNifBinary option;
    if (!enif_inspect_iolist_as_binary(env, argv[2], &option))
        return make_badarg(env, argv[2]);

    if (duckdb_set_config(config->config, (const char *)name.data, (const char *)option.data) == DuckDBError)
        return am_error;

    return am_ok;
}

static ERL_NIF_TERM
duxdb_destroy_config(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    config_t *config;
    if (!enif_get_resource(env, argv[0], config_type, (void **)&config))
        return make_badarg(env, argv[0]);

    if (config->config)
        duckdb_destroy_config(&config->config);

    config->config = NULL;
    return am_ok;
}

static ERL_NIF_TERM
duxdb_open_ext(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary path;
    if (!enif_inspect_iolist_as_binary(env, argv[0], &path))
        return make_badarg(env, argv[0]);

    config_t *config;
    if (!enif_get_resource(env, argv[1], config_type, (void **)&config) || !config->config)
        return make_badarg(env, argv[1]);

    db_t *db = enif_alloc_resource(db_type, sizeof(db_t));
    if (!db)
        return enif_raise_exception(env, am_system_limit);

    char *out_error;
    if (duckdb_open_ext((const char *)path.data, &db->db, config->config, &out_error) == DuckDBError)
    {
        db->db = NULL;
        enif_release_resource(db);
        ERL_NIF_TERM erl_error = make_binary(env, out_error, strlen(out_error));
        duckdb_free(out_error);
        return erl_error;
    }

    ERL_NIF_TERM result = enif_make_resource(env, db);
    enif_release_resource(db);
    return result;
}

static ERL_NIF_TERM
duxdb_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    db_t *db;
    if (!enif_get_resource(env, argv[0], db_type, (void **)&db))
        return make_badarg(env, argv[0]);

    if (db->db)
        duckdb_close(&db->db);

    db->db = NULL;
    return am_ok;
}

static ERL_NIF_TERM
duxdb_connect(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    db_t *db;
    if (!enif_get_resource(env, argv[0], db_type, (void **)&db) || !db->db)
        return make_badarg(env, argv[0]);

    conn_t *conn = enif_alloc_resource(conn_type, sizeof(conn_t));
    if (!conn)
        return enif_raise_exception(env, am_system_limit);

    if (duckdb_connect(db->db, &conn->conn) == DuckDBError)
    {
        conn->conn = NULL;
        enif_release_resource(conn);
        return make_badarg(env, argv[0]);
    }

    ERL_NIF_TERM result = enif_make_resource(env, conn);
    enif_release_resource(conn);
    return result;
}

static ERL_NIF_TERM
duxdb_disconnect(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    conn_t *conn;
    if (!enif_get_resource(env, argv[0], conn_type, (void **)&conn))
        return make_badarg(env, argv[0]);

    if (conn->conn)
        duckdb_disconnect(&conn->conn);

    conn->conn = NULL;
    return am_ok;
}

static ErlNifFunc nif_funcs[] = {
    {"library_version", 0, duxdb_library_version},
    {"create_config", 0, duxdb_create_config},
    {"config_count", 0, duxdb_config_count},
    {"get_config_flag", 1, duxdb_get_config_flag},
    {"set_config_nif", 3, duxdb_set_config},
    {"destroy_config", 1, duxdb_destroy_config},
    {"open_ext_nif", 2, duxdb_open_ext},
    {"close", 1, duxdb_close},
    {"connect", 1, duxdb_connect},
    {"disconnect", 1, duxdb_disconnect},
};

ERL_NIF_INIT(Elixir.DuxDB, nif_funcs, on_load, NULL, NULL, NULL)
