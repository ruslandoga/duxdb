#include <assert.h>
#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <erl_nif.h>
#include <duckdb.h>

static ERL_NIF_TERM am_ok;
static ERL_NIF_TERM am_error;
static ERL_NIF_TERM am_true;
static ERL_NIF_TERM am_false;
static ERL_NIF_TERM am_nil;
static ERL_NIF_TERM am_badarg;
static ERL_NIF_TERM am_system_limit;

static ErlNifResourceType *config_t;
static ErlNifResourceType *db_t;
static ErlNifResourceType *conn_t;
static ErlNifResourceType *result_t;
static ErlNifResourceType *data_chunk_t;
static ErlNifResourceType *stmt_t;

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

typedef struct
{
    duckdb_prepared_statement duck;
} duxdb_stmt;

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

static void
stmt_destructor(ErlNifEnv *env, void *arg)
{
    duckdb_prepared_statement stmt = ((duxdb_stmt *)arg)->duck;
    if (stmt)
    {
        duckdb_destroy_prepare(&stmt);
        assert(stmt == NULL);
    }
}

static int
on_load(ErlNifEnv *env, void **priv, ERL_NIF_TERM info)
{
    am_ok = enif_make_atom(env, "ok");
    am_error = enif_make_atom(env, "error");
    am_true = enif_make_atom(env, "true");
    am_false = enif_make_atom(env, "false");
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

    stmt_t = enif_open_resource_type(env, "duxdb", "stmt", stmt_destructor, ERL_NIF_RT_CREATE, NULL);
    if (!stmt_t)
        return -1;

    return 0;
}

static ERL_NIF_TERM
make_binary(ErlNifEnv *env, const char *bytes, size_t size)
{
    ERL_NIF_TERM bin;
    // TODO can fail?
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

static ERL_NIF_TERM
duxdb_column_name(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_result *result;
    if (!enif_get_resource(env, argv[0], result_t, (void **)&result) || !(result->duck.internal_data))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    const char *name = duckdb_column_name(&result->duck, idx);
    if (!name)
        return am_nil;

    return make_binary(env, name, strlen(name));
}

static ERL_NIF_TERM
duxdb_result_statement_type(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_result *result;
    if (!enif_get_resource(env, argv[0], result_t, (void **)&result) || !(result->duck.internal_data))
        return make_badarg(env, argv[0]);

    duckdb_statement_type type = duckdb_result_statement_type(result->duck);
    return enif_make_int(env, type);
}

static ERL_NIF_TERM
duxdb_column_count(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_result *result;
    if (!enif_get_resource(env, argv[0], result_t, (void **)&result) || !(result->duck.internal_data))
        return make_badarg(env, argv[0]);

    idx_t count = duckdb_column_count(&result->duck);
    return enif_make_uint64(env, count);
}

static ERL_NIF_TERM
duxdb_rows_changed(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_result *result;
    if (!enif_get_resource(env, argv[0], result_t, (void **)&result) || !(result->duck.internal_data))
        return make_badarg(env, argv[0]);

    idx_t count = duckdb_rows_changed(&result->duck);
    return enif_make_uint64(env, count);
}

static ERL_NIF_TERM
duxdb_result_return_type(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_result *result;
    if (!enif_get_resource(env, argv[0], result_t, (void **)&result) || !(result->duck.internal_data))
        return make_badarg(env, argv[0]);

    duckdb_result_type result_type = duckdb_result_return_type(result->duck);
    return enif_make_int(env, result_type);
}

static ERL_NIF_TERM
duxdb_fetch_chunk(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_result *result;
    if (!enif_get_resource(env, argv[0], result_t, (void **)&result) || !(result->duck.internal_data))
        return make_badarg(env, argv[0]);

    duxdb_data_chunk *chunk = enif_alloc_resource(data_chunk_t, sizeof(duxdb_data_chunk));
    if (!chunk)
        return enif_raise_exception(env, am_system_limit);

    chunk->duck = duckdb_fetch_chunk(result->duck);
    if (!chunk->duck)
    {
        enif_release_resource(chunk);
        return am_nil;
    }

    ERL_NIF_TERM chunk_resource = enif_make_resource(env, chunk);
    enif_release_resource(chunk);
    return chunk_resource;
}

static ERL_NIF_TERM
duxdb_destroy_data_chunk(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_data_chunk *chunk;
    if (!enif_get_resource(env, argv[0], data_chunk_t, (void **)&chunk))
        return make_badarg(env, argv[0]);

    if (chunk->duck)
    {
        duckdb_destroy_data_chunk(&chunk->duck);
        assert(chunk->duck == NULL);
    }

    return am_ok;
}

static ERL_NIF_TERM
duxdb_data_chunk_get_column_count(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_data_chunk *chunk;
    if (!enif_get_resource(env, argv[0], data_chunk_t, (void **)&chunk) || !(chunk->duck))
        return make_badarg(env, argv[0]);

    idx_t count = duckdb_data_chunk_get_column_count(chunk->duck);
    return enif_make_uint64(env, count);
}

static ERL_NIF_TERM
duxdb_data_chunk_get_vector(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_data_chunk *chunk;
    if (!enif_get_resource(env, argv[0], data_chunk_t, (void **)&chunk) || !(chunk->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    idx_t chunk_size = duckdb_data_chunk_get_size(chunk->duck);

    duckdb_vector vector = duckdb_data_chunk_get_vector(chunk->duck, idx);
    if (!vector)
        return make_badarg(env, argv[1]);

    uint64_t *validity = duckdb_vector_get_validity(vector);
    void *data = duckdb_vector_get_data(vector);

    duckdb_logical_type logical_type = duckdb_vector_get_column_type(vector);
    duckdb_type type = duckdb_get_type_id(logical_type);
    duckdb_destroy_logical_type(&logical_type);

    ERL_NIF_TERM terms[chunk_size];

    // TODO refactor
    for (idx_t i = 0; i < chunk_size; i++)
    {
        if (duckdb_validity_row_is_valid(validity, i))
        {
            switch (type)
            {
            case DUCKDB_TYPE_BOOLEAN:
            {
                terms[i] = ((bool *)data)[i] ? am_true : am_false;
                break;
            }

            case DUCKDB_TYPE_TINYINT:
            {
                terms[i] = enif_make_int(env, ((int8_t *)data)[i]);
                break;
            }

            case DUCKDB_TYPE_SMALLINT:
            {
                terms[i] = enif_make_int(env, ((int16_t *)data)[i]);
                break;
            }

            case DUCKDB_TYPE_INTEGER:
            {
                terms[i] = enif_make_int(env, ((int32_t *)data)[i]);
                break;
            }

            case DUCKDB_TYPE_BIGINT:
            {
                terms[i] = enif_make_int64(env, ((int64_t *)data)[i]);
                break;
            }

            case DUCKDB_TYPE_UTINYINT:
            {
                terms[i] = enif_make_uint(env, ((uint8_t *)data)[i]);
                break;
            }

            case DUCKDB_TYPE_USMALLINT:
            {
                terms[i] = enif_make_uint(env, ((uint16_t *)data)[i]);
                break;
            }

            case DUCKDB_TYPE_UINTEGER:
            {
                terms[i] = enif_make_uint(env, ((uint32_t *)data)[i]);
                break;
            }

            case DUCKDB_TYPE_UBIGINT:
            {
                terms[i] = enif_make_uint64(env, ((uint64_t *)data)[i]);
                break;
            }

            case DUCKDB_TYPE_FLOAT:
            {
                terms[i] = enif_make_double(env, ((float *)data)[i]);
                break;
            }

            case DUCKDB_TYPE_DOUBLE:
            {
                terms[i] = enif_make_double(env, ((double *)data)[i]);
                break;
            }

            case DUCKDB_TYPE_TIMESTAMP:
            {
                duckdb_timestamp ts = ((duckdb_timestamp *)data)[i];
                terms[i] = enif_make_uint64(env, ts.micros);
                break;
            }

            case DUCKDB_TYPE_DATE:
            {
                duckdb_date d = ((duckdb_date *)data)[i];
                duckdb_date_struct ds = duckdb_from_date(d);

                // TODO
                enif_make_map_from_arrays(
                    env,
                    (ERL_NIF_TERM[]){enif_make_atom(env, "__struct__"), enif_make_atom(env, "calendar"), enif_make_atom(env, "year"), enif_make_atom(env, "month"), enif_make_atom(env, "day")},
                    (ERL_NIF_TERM[]){enif_make_atom(env, "Elixir.Date"), enif_make_atom(env, "Elixir.Calendar.ISO"), enif_make_int(env, ds.year), enif_make_int(env, ds.month), enif_make_int(env, ds.day)},
                    5,
                    &terms[i]);

                break;
            }

            case DUCKDB_TYPE_TIME:
            {
                duckdb_time t = ((duckdb_time *)data)[i];
                terms[i] = enif_make_int64(env, t.micros);
                break;
            }

            case DUCKDB_TYPE_INTERVAL:
            {
                duckdb_interval in = ((duckdb_interval *)data)[i];
                terms[i] = enif_make_tuple3(env, enif_make_int(env, in.months), enif_make_int(env, in.days), enif_make_int64(env, in.micros));
                break;
            }

            case DUCKDB_TYPE_HUGEINT:
            {
                duckdb_hugeint hi = ((duckdb_hugeint *)data)[i];
                terms[i] = enif_make_tuple2(env, enif_make_int64(env, hi.upper), enif_make_uint64(env, hi.lower));
                break;
            }

            case DUCKDB_TYPE_UHUGEINT:
            {
                duckdb_uhugeint uhi = ((duckdb_uhugeint *)data)[i];
                terms[i] = enif_make_tuple2(env, enif_make_uint64(env, uhi.upper), enif_make_uint64(env, uhi.lower));
                break;
            }

            case DUCKDB_TYPE_VARCHAR:
            case DUCKDB_TYPE_BLOB:
            {
                duckdb_string_t str = ((duckdb_string_t *)data)[i];

                if (duckdb_string_is_inlined(str))
                {
                    terms[i] = make_binary(env, str.value.inlined.inlined, str.value.inlined.length);
                }
                else
                {
                    // TODO
                    terms[i] = make_binary(env, str.value.pointer.ptr, str.value.pointer.length);
                }

                break;
            }

            case DUCKDB_TYPE_DECIMAL:
            {
                terms[i] = am_nil;
                break;
            }

            case DUCKDB_TYPE_TIMESTAMP_S:
            {
                terms[i] = am_nil;
                break;
            }

            case DUCKDB_TYPE_TIMESTAMP_MS:
            {
                terms[i] = am_nil;
                break;
            }

            case DUCKDB_TYPE_TIMESTAMP_NS:
            {
                terms[i] = am_nil;
                break;
            }

            case DUCKDB_TYPE_ENUM:
            {
                terms[i] = am_nil;
                break;
            }

            case DUCKDB_TYPE_LIST:
            {
                terms[i] = am_nil;
                break;
            }

            case DUCKDB_TYPE_STRUCT:
            {
                terms[i] = am_nil;
                break;
            }

            case DUCKDB_TYPE_MAP:
            {
                terms[i] = am_nil;
                break;
            }

            case DUCKDB_TYPE_ARRAY:
            {
                terms[i] = am_nil;
                break;
            }

            case DUCKDB_TYPE_UUID:
            {
                terms[i] = am_nil;
                break;
            }

            case DUCKDB_TYPE_UNION:
            {
                terms[i] = am_nil;
                break;
            }

            case DUCKDB_TYPE_BIT:
            {
                terms[i] = am_nil;
                break;
            }

            case DUCKDB_TYPE_TIME_TZ:
            {
                terms[i] = am_nil;
                break;
            }

            case DUCKDB_TYPE_TIMESTAMP_TZ:
            {
                terms[i] = am_nil;
                break;
            }

            case DUCKDB_TYPE_ANY:
            {
                terms[i] = am_nil;
                break;
            }

            case DUCKDB_TYPE_VARINT:
            {
                terms[i] = am_nil;
                break;
            }

            case DUCKDB_TYPE_SQLNULL:
            {
                terms[i] = am_nil;
                break;
            }

            default:
            {
                // TODO raise exception
                terms[i] = am_nil;
                break;
            }
            }
        }
        else
        {
            terms[i] = am_nil;
        }
    }

    return enif_make_list_from_array(env, terms, chunk_size);
}

static ERL_NIF_TERM
duxdb_prepare_nif(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_conn *conn;
    if (!enif_get_resource(env, argv[0], conn_t, (void **)&conn) || !(conn->duck))
        return make_badarg(env, argv[0]);

    ErlNifBinary query;
    if (!enif_inspect_iolist_as_binary(env, argv[1], &query))
        return make_badarg(env, argv[1]);

    duxdb_stmt *stmt = enif_alloc_resource(stmt_t, sizeof(duxdb_stmt));
    if (!stmt)
        return enif_raise_exception(env, am_system_limit);

    if (duckdb_prepare(conn->duck, (const char *)query.data, &stmt->duck) == DuckDBError)
    {
        enif_release_resource(stmt);
        const char *error = duckdb_prepare_error(stmt->duck);
        return make_binary(env, error, strlen(error));
    }

    ERL_NIF_TERM stmt_resource = enif_make_resource(env, stmt);
    enif_release_resource(stmt);
    return stmt_resource;
}

static ERL_NIF_TERM
duxdb_destroy_prepare(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt))
        return make_badarg(env, argv[0]);

    if (stmt->duck)
    {
        duckdb_destroy_prepare(&stmt->duck);
        assert(stmt->duck == NULL);
    }

    return am_ok;
}

static ERL_NIF_TERM
duxdb_nparams(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    idx_t count = duckdb_nparams(stmt->duck);
    return enif_make_uint64(env, count);
}

static ERL_NIF_TERM
duxdb_parameter_name(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    const char *parameter_name = duckdb_parameter_name(stmt->duck, idx);
    if (!parameter_name)
        return make_badarg(env, argv[1]);

    ERL_NIF_TERM name = make_binary(env, parameter_name, strlen(parameter_name));
    // TODO
    duckdb_free((char *)parameter_name);
    return name;
}

static ERL_NIF_TERM
duxdb_bind_parameter_index(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifBinary name;
    if (!enif_inspect_iolist_as_binary(env, argv[1], &name))
        return make_badarg(env, argv[1]);

    idx_t idx;
    if (duckdb_bind_parameter_index(stmt->duck, &idx, (const char *)name.data) == DuckDBError)
        return make_badarg(env, argv[1]);

    return enif_make_uint64(env, idx);
}

static ERL_NIF_TERM
duxdb_clear_bindings(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    if (duckdb_clear_bindings(stmt->duck) == DuckDBError)
        return make_badarg(env, argv[0]);

    return am_ok;
}

static ERL_NIF_TERM
duxdb_prepared_statement_type(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    duckdb_statement_type type = duckdb_prepared_statement_type(stmt->duck);
    return enif_make_int(env, type);
}

static ERL_NIF_TERM
duxdb_execute_prepared(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    duxdb_result *result = enif_alloc_resource(result_t, sizeof(duxdb_result));
    if (!result)
        return enif_raise_exception(env, am_system_limit);

    if (duckdb_execute_prepared(stmt->duck, &result->duck) == DuckDBError)
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
duxdb_bind_boolean(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    // TODO
    duckdb_state rc;
    if (enif_is_identical(argv[2], am_true))
    {
        rc = duckdb_bind_boolean(stmt->duck, idx, 1);
    }
    else if (enif_is_identical(argv[2], am_false))
    {
        rc = duckdb_bind_boolean(stmt->duck, idx, 0);
    }
    else
    {
        return make_badarg(env, argv[2]);
    }

    if (rc == DuckDBError)
        return make_badarg(env, argv[2]);

    return am_ok;
}

static ERL_NIF_TERM
duxdb_bind_double(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    double value;
    if (!enif_get_double(env, argv[2], &value))
        return make_badarg(env, argv[2]);

    if (duckdb_bind_double(stmt->duck, idx, value) == DuckDBError)
        return make_badarg(env, argv[2]);

    return am_ok;
}

static ERL_NIF_TERM
duxdb_bind_int64(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    ErlNifSInt64 value;
    if (!enif_get_int64(env, argv[2], &value))
        return make_badarg(env, argv[2]);

    if (duckdb_bind_int64(stmt->duck, idx, value) == DuckDBError)
        return make_badarg(env, argv[2]);

    return am_ok;
}

static ERL_NIF_TERM
duxdb_bind_uint64(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    ErlNifUInt64 value;
    if (!enif_get_uint64(env, argv[2], &value))
        return make_badarg(env, argv[2]);

    if (duckdb_bind_uint64(stmt->duck, idx, value) == DuckDBError)
        return make_badarg(env, argv[2]);

    return am_ok;
}

static ERL_NIF_TERM
duxdb_bind_varchar(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    ErlNifBinary value;
    if (!enif_inspect_binary(env, argv[2], &value))
        return make_badarg(env, argv[2]);

    if (duckdb_bind_varchar_length(stmt->duck, idx, (const char *)value.data, value.size) == DuckDBError)
        return make_badarg(env, argv[2]);

    return am_ok;
}

static ERL_NIF_TERM
duxdb_bind_blob(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    ErlNifBinary value;
    if (!enif_inspect_binary(env, argv[2], &value))
        return make_badarg(env, argv[2]);

    if (duckdb_bind_blob(stmt->duck, idx, (const char *)value.data, value.size) == DuckDBError)
        return make_badarg(env, argv[2]);

    return am_ok;
}

static ERL_NIF_TERM
duxdb_bind_date(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    ErlNifSInt64 days;
    if (!enif_get_int64(env, argv[2], &days))
        return make_badarg(env, argv[2]);

    duckdb_date date = {.days = days};
    if (duckdb_bind_date(stmt->duck, idx, date) == DuckDBError)
        return make_badarg(env, argv[2]);

    return am_ok;
}

static ERL_NIF_TERM
duxdb_bind_null(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    if (duckdb_bind_null(stmt->duck, idx) == DuckDBError)
        return make_badarg(env, argv[2]);

    return am_ok;
}

static ErlNifFunc nif_funcs[] = {
    {"library_version", 0, duxdb_library_version, 0},

    {"create_config", 0, duxdb_create_config, 0},
    {"config_count", 0, duxdb_config_count, 0},
    {"get_config_flag", 1, duxdb_get_config_flag, 0},
    {"set_config_nif", 3, duxdb_set_config, 0},
    {"destroy_config", 1, duxdb_destroy_config, 0},

    {"open_ext_nif", 2, duxdb_open_ext, 0},
    {"open_ext_dirty_io_nif", 2, duxdb_open_ext, ERL_NIF_DIRTY_JOB_IO_BOUND},

    {"close", 1, duxdb_close, 0},
    {"close_dirty_io", 1, duxdb_close, ERL_NIF_DIRTY_JOB_IO_BOUND},

    {"connect", 1, duxdb_connect, 0},
    {"interrupt", 1, duxdb_interrupt, 0},
    {"query_progress", 1, duxdb_query_progress, 0},
    {"disconnect", 1, duxdb_disconnect, 0},

    {"query_nif", 2, duxdb_query, 0},
    {"query_dirty_io_nif", 2, duxdb_query, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"query_dirty_cpu_nif", 2, duxdb_query, ERL_NIF_DIRTY_JOB_CPU_BOUND},

    {"destroy_result", 1, duxdb_destroy_result, 0},
    {"column_name", 2, duxdb_column_name, 0},
    {"result_statement_type", 1, duxdb_result_statement_type, 0},
    {"column_count", 1, duxdb_column_count, 0},
    {"rows_changed", 1, duxdb_rows_changed, 0},
    {"result_return_type", 1, duxdb_result_return_type, 0},

    {"fetch_chunk", 1, duxdb_fetch_chunk, 0},
    {"destroy_data_chunk", 1, duxdb_destroy_data_chunk, 0},
    {"data_chunk_get_column_count", 1, duxdb_data_chunk_get_column_count, 0},
    {"data_chunk_get_vector", 2, duxdb_data_chunk_get_vector, 0},

    {"prepare_nif", 2, duxdb_prepare_nif, 0},
    {"prepare_dirty_cpu_nif", 2, duxdb_prepare_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},

    {"destroy_prepare", 1, duxdb_destroy_prepare, 0},
    {"nparams", 1, duxdb_nparams, 0},
    {"parameter_name", 2, duxdb_parameter_name, 0},
    {"bind_parameter_index_nif", 2, duxdb_bind_parameter_index, 0},
    {"clear_bindings", 1, duxdb_clear_bindings, 0},
    {"prepared_statement_type", 1, duxdb_prepared_statement_type, 0},

    {"execute_prepared_nif", 1, duxdb_execute_prepared, 0},
    {"execute_prepared_dirty_io_nif", 1, duxdb_execute_prepared, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"execute_prepared_dirty_cpu_nif", 1, duxdb_execute_prepared, ERL_NIF_DIRTY_JOB_CPU_BOUND},

    {"bind_boolean", 3, duxdb_bind_boolean, 0},
    {"bind_double", 3, duxdb_bind_double, 0},
    {"bind_int64", 3, duxdb_bind_int64, 0},
    {"bind_uint64", 3, duxdb_bind_uint64, 0},
    {"bind_varchar", 3, duxdb_bind_varchar, 0},
    {"bind_blob", 3, duxdb_bind_blob, 0},
    {"bind_date_nif", 3, duxdb_bind_date, 0},
    {"bind_null", 2, duxdb_bind_null, 0},
};

ERL_NIF_INIT(Elixir.DuxDB, nif_funcs, on_load, NULL, NULL, NULL)
