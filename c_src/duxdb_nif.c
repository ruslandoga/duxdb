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

static ErlNifResourceType *config_t;
static ErlNifResourceType *db_t;
static ErlNifResourceType *conn_t;
static ErlNifResourceType *result_t;
static ErlNifResourceType *data_chunk_t;

typedef struct
{
    duckdb_config duck;
} duxdb_config;

typedef struct
{
    duckdb_database duck;
} duxdb_db;

typedef struct
{
    duckdb_connection duck;
} duxdb_conn;

typedef struct
{
    duckdb_result duck;
} duxdb_result;

typedef struct
{
    duckdb_data_chunk duck;
} duxdb_data_chunk;

static void
config_destructor(ErlNifEnv *env, void *arg)
{
    duckdb_config config = ((duxdb_config *)arg)->duck;
    if (config)
    {
        duckdb_destroy_config(&config);
        assert(config == NULL);
    }
}

static void
db_destructor(ErlNifEnv *env, void *arg)
{
    duckdb_database db = ((duxdb_db *)arg)->duck;
    if (db)
    {
        duckdb_close(&db);
        assert(db == NULL);
    }
}

static void
conn_destructor(ErlNifEnv *env, void *arg)
{
    duckdb_connection conn = ((duxdb_conn *)arg)->duck;
    if (conn)
    {
        duckdb_disconnect(&conn);
        assert(conn == NULL);
    }
}

static void
result_destructor(ErlNifEnv *env, void *arg)
{
    duckdb_result result = ((duxdb_result *)arg)->duck;
    if (result.internal_data)
    {
        duckdb_destroy_result(&result);
        assert(result.internal_data == NULL);
    }
}

static void
data_chunk_destructor(ErlNifEnv *env, void *arg)
{
    duckdb_data_chunk chunk = ((duxdb_data_chunk *)arg)->duck;
    if (chunk)
    {
        duckdb_destroy_data_chunk(&chunk);
        assert(chunk == NULL);
    }
}

static int
on_load(ErlNifEnv *env, void **priv, ERL_NIF_TERM info)
{
    am_ok = enif_make_atom(env, "ok");
    am_error = enif_make_atom(env, "error");
    am_nil = enif_make_atom(env, "nil");
    am_badarg = enif_make_atom(env, "badarg");
    am_system_limit = enif_make_atom(env, "system_limit");

    config_t = enif_open_resource_type(env, "duxdb", "config", config_destructor, ERL_NIF_RT_CREATE, NULL);
    if (!config_t)
        return -1;

    db_t = enif_open_resource_type(env, "duxdb", "db", db_destructor, ERL_NIF_RT_CREATE, NULL);
    if (!db_t)
        return -1;

    conn_t = enif_open_resource_type(env, "duxdb", "conn", conn_destructor, ERL_NIF_RT_CREATE, NULL);
    if (!conn_t)
        return -1;

    result_t = enif_open_resource_type(env, "duxdb", "result", result_destructor, ERL_NIF_RT_CREATE, NULL);
    if (!result_t)
        return -1;

    data_chunk_t = enif_open_resource_type(env, "duxdb", "data_chunk", data_chunk_destructor, ERL_NIF_RT_CREATE, NULL);
    if (!data_chunk_t)
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
    duxdb_config *config = enif_alloc_resource(config_t, sizeof(duxdb_config));
    if (!config)
        return enif_raise_exception(env, am_system_limit);

    if (duckdb_create_config(&config->duck) == DuckDBError)
    {
        enif_release_resource(config);
        duckdb_destroy_config(&config->duck);
        assert(config->duck == NULL);
        return enif_raise_exception(env, am_system_limit);
    }

    ERL_NIF_TERM config_resource = enif_make_resource(env, config);
    enif_release_resource(config);
    return config_resource;
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
    duxdb_config *config;
    if (!enif_get_resource(env, argv[0], config_t, (void **)&config) || !(config->duck))
        return make_badarg(env, argv[0]);

    ErlNifBinary name;
    if (!enif_inspect_iolist_as_binary(env, argv[1], &name))
        return make_badarg(env, argv[1]);

    ErlNifBinary option;
    if (!enif_inspect_iolist_as_binary(env, argv[2], &option))
        return make_badarg(env, argv[2]);

    if (duckdb_set_config(config->duck, (const char *)name.data, (const char *)option.data) == DuckDBError)
        return am_error;

    return am_ok;
}

static ERL_NIF_TERM
duxdb_destroy_config(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_config *config;
    if (!enif_get_resource(env, argv[0], config_t, (void **)&config))
        return make_badarg(env, argv[0]);

    if (config->duck)
    {
        duckdb_destroy_config(&config->duck);
        assert(config->duck == NULL);
    }

    return am_ok;
}

static ERL_NIF_TERM
duxdb_open_ext(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    ErlNifBinary path;
    if (!enif_inspect_iolist_as_binary(env, argv[0], &path))
        return make_badarg(env, argv[0]);

    duxdb_config *config;
    if (!enif_get_resource(env, argv[1], config_t, (void **)&config) || !(config->duck))
        return make_badarg(env, argv[1]);

    duxdb_db *db = enif_alloc_resource(db_t, sizeof(duxdb_db));
    if (!db)
        return enif_raise_exception(env, am_system_limit);

    // sometimes enif_alloc_resource allocates non-zeroed memory
    // which messes up destructors, so we need to zero it out
    db->duck = NULL;

    char *errstr;
    if (duckdb_open_ext((const char *)path.data, &db->duck, config->duck, &errstr) == DuckDBError)
    {
        assert(db->duck == NULL);
        enif_release_resource(db);

        ERL_NIF_TERM error = make_binary(env, errstr, strlen(errstr));
        duckdb_free(errstr);

        return error;
    }

    ERL_NIF_TERM db_resource = enif_make_resource(env, db);
    enif_release_resource(db);
    return db_resource;
}

static ERL_NIF_TERM
duxdb_close(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_db *db;
    if (!enif_get_resource(env, argv[0], db_t, (void **)&db))
        return make_badarg(env, argv[0]);

    if (db->duck)
    {
        duckdb_close(&db->duck);
        assert(db->duck == NULL);
    }

    return am_ok;
}

static ERL_NIF_TERM
duxdb_connect(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_db *db;
    if (!enif_get_resource(env, argv[0], db_t, (void **)&db) || !(db->duck))
        return make_badarg(env, argv[0]);

    duxdb_conn *conn = enif_alloc_resource(conn_t, sizeof(duxdb_conn));
    if (!conn)
        return enif_raise_exception(env, am_system_limit);

    // sometimes enif_alloc_resource allocates non-zeroed memory
    // which messes up destructors, so we need to zero it out
    conn->duck = NULL;

    if (duckdb_connect(db->duck, &conn->duck) == DuckDBError)
    {
        assert(conn->duck == NULL);
        enif_release_resource(conn);
        return enif_raise_exception(env, am_system_limit);
    }

    ERL_NIF_TERM conn_resource = enif_make_resource(env, conn);
    enif_release_resource(conn);
    return conn_resource;
}

static ERL_NIF_TERM
duxdb_interrupt(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_conn *conn;
    if (!enif_get_resource(env, argv[0], conn_t, (void **)&conn) || !(conn->duck))
        return make_badarg(env, argv[0]);

    duckdb_interrupt(conn->duck);
    return am_ok;
}

static ERL_NIF_TERM
duxdb_query_progress(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_conn *conn;
    if (!enif_get_resource(env, argv[0], conn_t, (void **)&conn) || !(conn->duck))
        return make_badarg(env, argv[0]);

    duckdb_query_progress_type progress = duckdb_query_progress(conn->duck);

    ERL_NIF_TERM percentage = enif_make_double(env, progress.percentage);
    ERL_NIF_TERM rows_processed = enif_make_uint64(env, progress.rows_processed);
    ERL_NIF_TERM total_rows_to_process = enif_make_uint64(env, progress.total_rows_to_process);

    return enif_make_tuple3(env, percentage, rows_processed, total_rows_to_process);
}

static ERL_NIF_TERM
duxdb_disconnect(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_conn *conn;
    if (!enif_get_resource(env, argv[0], conn_t, (void **)&conn))
        return make_badarg(env, argv[0]);

    if (conn->duck)
    {
        duckdb_disconnect(&conn->duck);
        assert(conn->duck == NULL);
    }

    return am_ok;
}

static ERL_NIF_TERM
duxdb_query(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_conn *conn;
    if (!enif_get_resource(env, argv[0], conn_t, (void **)&conn) || !(conn->duck))
        return make_badarg(env, argv[0]);

    ErlNifBinary query;
    if (!enif_inspect_iolist_as_binary(env, argv[1], &query))
        return make_badarg(env, argv[1]);

    duxdb_result *result = enif_alloc_resource(result_t, sizeof(duxdb_result));
    if (!result)
        return enif_raise_exception(env, am_system_limit);

    if (duckdb_query(conn->duck, (const char *)query.data, &result->duck) == DuckDBError)
    {
        enif_release_resource(result);

        int error_type = (int)duckdb_result_error_type(&result->duck);
        ERL_NIF_TERM errc = enif_make_int(env, error_type);

        const char *error = duckdb_result_error(&result->duck);
        ERL_NIF_TERM errmsg = make_binary(env, error, strlen(error));

        duckdb_destroy_result(&result->duck);
        assert(result->duck.internal_data == NULL);

        return enif_make_tuple2(env, errc, errmsg);
    }

    ERL_NIF_TERM result_resource = enif_make_resource(env, result);
    enif_release_resource(result);
    return result_resource;
}

static ERL_NIF_TERM
duxdb_destroy_result(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_result *result;
    if (!enif_get_resource(env, argv[0], result_t, (void **)&result))
        return make_badarg(env, argv[0]);

    if (result->duck.internal_data)
    {
        duckdb_destroy_result(&result->duck);
        assert(result->duck.internal_data == NULL);
    }

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
    {"open_ext_dirty_io_nif", 2, duxdb_open_ext, ERL_NIF_DIRTY_JOB_IO_BOUND},

    {"close", 1, duxdb_close},
    {"close_dirty_io", 1, duxdb_close, ERL_NIF_DIRTY_JOB_IO_BOUND},

    {"connect", 1, duxdb_connect},
    {"interrupt", 1, duxdb_interrupt},
    {"query_progress", 1, duxdb_query_progress},
    {"disconnect", 1, duxdb_disconnect},

    {"query_nif", 2, duxdb_query},
    {"query_dirty_io_nif", 2, duxdb_query, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"query_dirty_cpu_nif", 2, duxdb_query, ERL_NIF_DIRTY_JOB_CPU_BOUND},

    {"destroy_result", 1, duxdb_destroy_result},
};

ERL_NIF_INIT(Elixir.DuxDB, nif_funcs, on_load, NULL, NULL, NULL)
