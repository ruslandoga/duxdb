#include <assert.h>
#include <stdint.h>
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
static ERL_NIF_TERM am___struct__;
static ERL_NIF_TERM am_calendar;
static ERL_NIF_TERM am_year;
static ERL_NIF_TERM am_month;
static ERL_NIF_TERM am_day;
static ERL_NIF_TERM am_hour;
static ERL_NIF_TERM am_minute;
static ERL_NIF_TERM am_second;
static ERL_NIF_TERM am_microsecond;
static ERL_NIF_TERM am_elixir_date;
static ERL_NIF_TERM am_elixir_time;
static ERL_NIF_TERM am_elixir_naive_date_time;
static ERL_NIF_TERM am_elixir_calendar_iso;

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
    am___struct__ = enif_make_atom(env, "__struct__");
    am_calendar = enif_make_atom(env, "calendar");
    am_year = enif_make_atom(env, "year");
    am_month = enif_make_atom(env, "month");
    am_day = enif_make_atom(env, "day");
    am_hour = enif_make_atom(env, "hour");
    am_minute = enif_make_atom(env, "minute");
    am_second = enif_make_atom(env, "second");
    am_microsecond = enif_make_atom(env, "microsecond");
    am_elixir_date = enif_make_atom(env, "Elixir.Date");
    am_elixir_time = enif_make_atom(env, "Elixir.Time");
    am_elixir_naive_date_time = enif_make_atom(env, "Elixir.NaiveDateTime");
    am_elixir_calendar_iso = enif_make_atom(env, "Elixir.Calendar.ISO");

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

// TODO inline
// TODO finite / infinite
static ERL_NIF_TERM
make_date(ErlNifEnv *env, const duckdb_date_struct date_struct)
{
    ERL_NIF_TERM date;

    // TODO handle error
    enif_make_map_from_arrays(
        env,
        (ERL_NIF_TERM[]){
            am___struct__,
            am_calendar,
            am_year,
            am_month,
            am_day},
        (ERL_NIF_TERM[]){
            am_elixir_date,
            am_elixir_calendar_iso,
            enif_make_int(env, date_struct.year),
            enif_make_int(env, date_struct.month),
            enif_make_int(env, date_struct.day)},
        5,
        &date);

    return date;
}

// // TODO
// int micros_precision(int32_t microseconds)
// {
//     if (microseconds == 0)
//         return 0;
//     if (microseconds % 1000 == 0)
//         return 3;
//     return 6;
// }

// TODO inline
static ERL_NIF_TERM
make_time(ErlNifEnv *env, const duckdb_time_struct time_struct)
{
    ERL_NIF_TERM time;

    // TODO handle error
    enif_make_map_from_arrays(
        env,
        (ERL_NIF_TERM[]){
            am___struct__,
            am_calendar,
            am_hour,
            am_minute,
            am_second,
            am_microsecond},
        (ERL_NIF_TERM[]){
            am_elixir_time,
            am_elixir_calendar_iso,
            enif_make_int(env, time_struct.hour),
            enif_make_int(env, time_struct.min),
            enif_make_int(env, time_struct.sec),
            enif_make_tuple2(env, enif_make_int(env, time_struct.micros), enif_make_int(env, 6))},
        6,
        &time);

    return time;
}

// TODO inline
// TODO finite / infinite
static ERL_NIF_TERM
make_naive_datetime(ErlNifEnv *env, const duckdb_timestamp_struct ts)
{

    duckdb_date_struct d = ts.date;
    duckdb_time_struct t = ts.time;

    ERL_NIF_TERM datetime;

    // TODO handle error
    enif_make_map_from_arrays(
        env,
        (ERL_NIF_TERM[]){
            am___struct__,
            am_calendar,
            am_year,
            am_month,
            am_day,
            am_hour,
            am_minute,
            am_second,
            am_microsecond},
        (ERL_NIF_TERM[]){
            am_elixir_naive_date_time,
            am_elixir_calendar_iso,
            enif_make_int(env, d.year),
            enif_make_int(env, d.month),
            enif_make_int(env, d.day),
            enif_make_int(env, t.hour),
            enif_make_int(env, t.min),
            enif_make_int(env, t.sec),
            enif_make_tuple2(env, enif_make_int(env, t.micros), enif_make_int(env, 6))},
        9,
        &datetime);

    return datetime;
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

// TODO: this are silly, maybe macros?

static ERL_NIF_TERM
make_bools_from_vector(ErlNifEnv *env, idx_t chunk_size, duckdb_vector vector)
{
    ERL_NIF_TERM terms[chunk_size];
    bool *data = (bool *)duckdb_vector_get_data(vector);
    uint64_t *validity = duckdb_vector_get_validity(vector);

    if (validity)
    {
        for (idx_t i = 0; i < chunk_size; i++)
            if (duckdb_validity_row_is_valid(validity, i))
                terms[i] = data[i] ? am_true : am_false;
            else
                terms[i] = am_nil;
    }
    else
    {
        for (idx_t i = 0; i < chunk_size; i++)
            terms[i] = data[i] ? am_true : am_false;
    }

    return enif_make_list_from_array(env, terms, chunk_size);
}

static ERL_NIF_TERM
make_int8s_from_vector(ErlNifEnv *env, idx_t chunk_size, duckdb_vector vector)
{
    ERL_NIF_TERM terms[chunk_size];
    int8_t *data = (int8_t *)duckdb_vector_get_data(vector);
    uint64_t *validity = duckdb_vector_get_validity(vector);

    if (validity)
    {
        for (idx_t i = 0; i < chunk_size; i++)
            if (duckdb_validity_row_is_valid(validity, i))
                terms[i] = enif_make_int(env, data[i]);
            else
                terms[i] = am_nil;
    }
    else
    {
        for (idx_t i = 0; i < chunk_size; i++)
            terms[i] = enif_make_int(env, data[i]);
    }

    return enif_make_list_from_array(env, terms, chunk_size);
}

static ERL_NIF_TERM
make_int16s_from_vector(ErlNifEnv *env, idx_t chunk_size, duckdb_vector vector)
{
    ERL_NIF_TERM terms[chunk_size];
    int16_t *data = (int16_t *)duckdb_vector_get_data(vector);
    uint64_t *validity = duckdb_vector_get_validity(vector);

    if (validity)
    {
        for (idx_t i = 0; i < chunk_size; i++)
            if (duckdb_validity_row_is_valid(validity, i))
                terms[i] = enif_make_int(env, data[i]);
            else
                terms[i] = am_nil;
    }
    else
    {
        for (idx_t i = 0; i < chunk_size; i++)
            terms[i] = enif_make_int(env, data[i]);
    }

    return enif_make_list_from_array(env, terms, chunk_size);
}

static ERL_NIF_TERM
make_int32s_from_vector(ErlNifEnv *env, idx_t chunk_size, duckdb_vector vector)
{
    ERL_NIF_TERM terms[chunk_size];
    int32_t *data = (int32_t *)duckdb_vector_get_data(vector);
    uint64_t *validity = duckdb_vector_get_validity(vector);

    if (validity)
    {
        for (idx_t i = 0; i < chunk_size; i++)
            if (duckdb_validity_row_is_valid(validity, i))
                terms[i] = enif_make_int(env, data[i]);
            else
                terms[i] = am_nil;
    }
    else
    {
        for (idx_t i = 0; i < chunk_size; i++)
            terms[i] = enif_make_int(env, data[i]);
    }

    return enif_make_list_from_array(env, terms, chunk_size);
}

static ERL_NIF_TERM
make_int64s_from_vector(ErlNifEnv *env, idx_t chunk_size, duckdb_vector vector)
{
    ERL_NIF_TERM terms[chunk_size];
    int64_t *data = (int64_t *)duckdb_vector_get_data(vector);
    uint64_t *validity = duckdb_vector_get_validity(vector);

    if (validity)
    {
        for (idx_t i = 0; i < chunk_size; i++)
            if (duckdb_validity_row_is_valid(validity, i))
                terms[i] = enif_make_int64(env, data[i]);
            else
                terms[i] = am_nil;
    }
    else
    {
        for (idx_t i = 0; i < chunk_size; i++)
            terms[i] = enif_make_int64(env, data[i]);
    }

    return enif_make_list_from_array(env, terms, chunk_size);
}

static ERL_NIF_TERM
make_uint8s_from_vector(ErlNifEnv *env, idx_t chunk_size, duckdb_vector vector)
{
    ERL_NIF_TERM terms[chunk_size];
    uint8_t *data = (uint8_t *)duckdb_vector_get_data(vector);
    uint64_t *validity = duckdb_vector_get_validity(vector);

    if (validity)
    {
        for (idx_t i = 0; i < chunk_size; i++)
            if (duckdb_validity_row_is_valid(validity, i))
                terms[i] = enif_make_uint(env, data[i]);
            else
                terms[i] = am_nil;
    }
    else
    {
        for (idx_t i = 0; i < chunk_size; i++)
            terms[i] = enif_make_uint(env, data[i]);
    }

    return enif_make_list_from_array(env, terms, chunk_size);
}

static ERL_NIF_TERM
make_uint16s_from_vector(ErlNifEnv *env, idx_t chunk_size, duckdb_vector vector)
{
    ERL_NIF_TERM terms[chunk_size];
    uint16_t *data = (uint16_t *)duckdb_vector_get_data(vector);
    uint64_t *validity = duckdb_vector_get_validity(vector);

    if (validity)
    {
        for (idx_t i = 0; i < chunk_size; i++)
            if (duckdb_validity_row_is_valid(validity, i))
                terms[i] = enif_make_uint(env, data[i]);
            else
                terms[i] = am_nil;
    }
    else
    {
        for (idx_t i = 0; i < chunk_size; i++)
            terms[i] = enif_make_uint(env, data[i]);
    }

    return enif_make_list_from_array(env, terms, chunk_size);
}

static ERL_NIF_TERM
make_uint32s_from_vector(ErlNifEnv *env, idx_t chunk_size, duckdb_vector vector)
{
    ERL_NIF_TERM terms[chunk_size];
    uint32_t *data = (uint32_t *)duckdb_vector_get_data(vector);
    uint64_t *validity = duckdb_vector_get_validity(vector);

    if (validity)
    {
        for (idx_t i = 0; i < chunk_size; i++)
            if (duckdb_validity_row_is_valid(validity, i))
                terms[i] = enif_make_uint(env, data[i]);
            else
                terms[i] = am_nil;
    }
    else
    {
        for (idx_t i = 0; i < chunk_size; i++)
            terms[i] = enif_make_uint(env, data[i]);
    }

    return enif_make_list_from_array(env, terms, chunk_size);
}

static ERL_NIF_TERM
make_uint64s_from_vector(ErlNifEnv *env, idx_t chunk_size, duckdb_vector vector)
{
    ERL_NIF_TERM terms[chunk_size];
    uint64_t *data = (uint64_t *)duckdb_vector_get_data(vector);
    uint64_t *validity = duckdb_vector_get_validity(vector);

    if (validity)
    {
        for (idx_t i = 0; i < chunk_size; i++)
            if (duckdb_validity_row_is_valid(validity, i))
                terms[i] = enif_make_uint64(env, data[i]);
            else
                terms[i] = am_nil;
    }
    else
    {
        for (idx_t i = 0; i < chunk_size; i++)
            terms[i] = enif_make_uint64(env, data[i]);
    }

    return enif_make_list_from_array(env, terms, chunk_size);
}

static ERL_NIF_TERM
make_f32s_from_vector(ErlNifEnv *env, idx_t chunk_size, duckdb_vector vector)
{
    ERL_NIF_TERM terms[chunk_size];
    float *data = (float *)duckdb_vector_get_data(vector);
    uint64_t *validity = duckdb_vector_get_validity(vector);

    if (validity)
    {
        for (idx_t i = 0; i < chunk_size; i++)
            if (duckdb_validity_row_is_valid(validity, i))
                terms[i] = enif_make_double(env, data[i]);
            else
                terms[i] = am_nil;
    }
    else
    {
        for (idx_t i = 0; i < chunk_size; i++)
            terms[i] = enif_make_double(env, data[i]);
    }

    return enif_make_list_from_array(env, terms, chunk_size);
}

static ERL_NIF_TERM
make_f64s_from_vector(ErlNifEnv *env, idx_t chunk_size, duckdb_vector vector)
{
    ERL_NIF_TERM terms[chunk_size];
    double *data = (double *)duckdb_vector_get_data(vector);
    uint64_t *validity = duckdb_vector_get_validity(vector);

    if (validity)
    {
        for (idx_t i = 0; i < chunk_size; i++)
            if (duckdb_validity_row_is_valid(validity, i))
                terms[i] = enif_make_double(env, data[i]);
            else
                terms[i] = am_nil;
    }
    else
    {
        for (idx_t i = 0; i < chunk_size; i++)
            terms[i] = enif_make_double(env, data[i]);
    }

    return enif_make_list_from_array(env, terms, chunk_size);
}

static ERL_NIF_TERM
make_dates_from_vector(ErlNifEnv *env, idx_t chunk_size, duckdb_vector vector)
{
    ERL_NIF_TERM terms[chunk_size];
    duckdb_date *data = (duckdb_date *)duckdb_vector_get_data(vector);
    uint64_t *validity = duckdb_vector_get_validity(vector);

    if (validity)
    {
        for (idx_t i = 0; i < chunk_size; i++)
            if (duckdb_validity_row_is_valid(validity, i))
                terms[i] = make_date(env, duckdb_from_date(data[i]));
            else
                terms[i] = am_nil;
    }
    else
    {
        for (idx_t i = 0; i < chunk_size; i++)
            terms[i] = make_date(env, duckdb_from_date(data[i]));
    }

    return enif_make_list_from_array(env, terms, chunk_size);
}

static ERL_NIF_TERM
make_times_from_vector(ErlNifEnv *env, idx_t chunk_size, duckdb_vector vector)
{
    ERL_NIF_TERM terms[chunk_size];
    duckdb_time *data = (duckdb_time *)duckdb_vector_get_data(vector);
    uint64_t *validity = duckdb_vector_get_validity(vector);

    if (validity)
    {
        for (idx_t i = 0; i < chunk_size; i++)
            if (duckdb_validity_row_is_valid(validity, i))
                terms[i] = make_time(env, duckdb_from_time(data[i]));
            else
                terms[i] = am_nil;
    }
    else
    {
        for (idx_t i = 0; i < chunk_size; i++)
            terms[i] = make_time(env, duckdb_from_time(data[i]));
    }

    return enif_make_list_from_array(env, terms, chunk_size);
}

static ERL_NIF_TERM
make_binaries_from_vector(ErlNifEnv *env, idx_t chunk_size, duckdb_vector vector)
{
    ERL_NIF_TERM terms[chunk_size];
    duckdb_string_t *data = (duckdb_string_t *)duckdb_vector_get_data(vector);
    uint64_t *validity = duckdb_vector_get_validity(vector);

    if (validity)
    {
        for (idx_t i = 0; i < chunk_size; i++)
            if (duckdb_validity_row_is_valid(validity, i))
            {
                duckdb_string_t str = data[i];
                terms[i] = make_binary(env, duckdb_string_t_data(&str), duckdb_string_t_length(str));
            }
            else
                terms[i] = am_nil;
    }
    else
    {
        for (idx_t i = 0; i < chunk_size; i++)
        {
            duckdb_string_t str = data[i];
            terms[i] = make_binary(env, duckdb_string_t_data(&str), duckdb_string_t_length(str));
        }
    }

    return enif_make_list_from_array(env, terms, chunk_size);
}

static ERL_NIF_TERM
make_naive_datetimes_from_vector(ErlNifEnv *env, idx_t chunk_size, duckdb_vector vector)
{
    ERL_NIF_TERM terms[chunk_size];
    duckdb_timestamp *data = (duckdb_timestamp *)duckdb_vector_get_data(vector);
    uint64_t *validity = duckdb_vector_get_validity(vector);

    if (validity)
    {
        for (idx_t i = 0; i < chunk_size; i++)
            if (duckdb_validity_row_is_valid(validity, i))
                terms[i] = make_naive_datetime(env, duckdb_from_timestamp(data[i]));
            else
                terms[i] = am_nil;
    }
    else
    {
        for (idx_t i = 0; i < chunk_size; i++)
            terms[i] = make_naive_datetime(env, duckdb_from_timestamp(data[i]));
    }

    return enif_make_list_from_array(env, terms, chunk_size);
}

// TODO inline
static ERL_NIF_TERM
make_hugeint(ErlNifEnv *env, duckdb_hugeint hi)
{
    return enif_make_tuple2(env, enif_make_int64(env, hi.upper), enif_make_uint64(env, hi.lower));
}

// TODO inline
static ERL_NIF_TERM
make_uhugeint(ErlNifEnv *env, duckdb_uhugeint hi)
{
    return enif_make_tuple2(env, enif_make_uint64(env, hi.upper), enif_make_uint64(env, hi.lower));
}

static ERL_NIF_TERM
make_int128s_from_vector(ErlNifEnv *env, idx_t chunk_size, duckdb_vector vector)
{
    ERL_NIF_TERM terms[chunk_size];
    duckdb_hugeint *data = (duckdb_hugeint *)duckdb_vector_get_data(vector);
    uint64_t *validity = duckdb_vector_get_validity(vector);

    if (validity)
    {
        for (idx_t i = 0; i < chunk_size; i++)
            if (duckdb_validity_row_is_valid(validity, i))
                terms[i] = make_hugeint(env, data[i]);
            else
                terms[i] = am_nil;
    }
    else
    {
        for (idx_t i = 0; i < chunk_size; i++)
            terms[i] = make_hugeint(env, data[i]);
    }

    return enif_make_list_from_array(env, terms, chunk_size);
}

static ERL_NIF_TERM
make_uint128s_from_vector(ErlNifEnv *env, idx_t chunk_size, duckdb_vector vector)
{
    ERL_NIF_TERM terms[chunk_size];
    duckdb_uhugeint *data = (duckdb_uhugeint *)duckdb_vector_get_data(vector);
    uint64_t *validity = duckdb_vector_get_validity(vector);

    if (validity)
    {
        for (idx_t i = 0; i < chunk_size; i++)
            if (duckdb_validity_row_is_valid(validity, i))
                terms[i] = make_uhugeint(env, data[i]);
            else
                terms[i] = am_nil;
    }
    else
    {
        for (idx_t i = 0; i < chunk_size; i++)
            terms[i] = make_uhugeint(env, data[i]);
    }

    return enif_make_list_from_array(env, terms, chunk_size);
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

    duckdb_logical_type logical_type = duckdb_vector_get_column_type(vector);
    duckdb_type type = duckdb_get_type_id(logical_type);
    duckdb_destroy_logical_type(&logical_type);

    switch (type)
    {
    case DUCKDB_TYPE_BOOLEAN:
        return make_bools_from_vector(env, chunk_size, vector);

    case DUCKDB_TYPE_INTEGER:
        return make_int32s_from_vector(env, chunk_size, vector);
    case DUCKDB_TYPE_UINTEGER:
        return make_uint32s_from_vector(env, chunk_size, vector);

    case DUCKDB_TYPE_VARCHAR:
    case DUCKDB_TYPE_BLOB:
        return make_binaries_from_vector(env, chunk_size, vector);

    case DUCKDB_TYPE_BIGINT:
        return make_int64s_from_vector(env, chunk_size, vector);
    case DUCKDB_TYPE_UBIGINT:
        return make_uint64s_from_vector(env, chunk_size, vector);

    case DUCKDB_TYPE_FLOAT:
        return make_f32s_from_vector(env, chunk_size, vector);
    case DUCKDB_TYPE_DOUBLE:
        return make_f64s_from_vector(env, chunk_size, vector);

    case DUCKDB_TYPE_TIMESTAMP:
    case DUCKDB_TYPE_TIMESTAMP_S:
    case DUCKDB_TYPE_TIMESTAMP_MS:
    case DUCKDB_TYPE_TIMESTAMP_NS:
        return make_naive_datetimes_from_vector(env, chunk_size, vector);

    case DUCKDB_TYPE_DATE:
        return make_dates_from_vector(env, chunk_size, vector);
    case DUCKDB_TYPE_TIME:
        return make_times_from_vector(env, chunk_size, vector);

    case DUCKDB_TYPE_TINYINT:
        return make_int8s_from_vector(env, chunk_size, vector);
    case DUCKDB_TYPE_SMALLINT:
        return make_int16s_from_vector(env, chunk_size, vector);

    case DUCKDB_TYPE_UTINYINT:
        return make_uint8s_from_vector(env, chunk_size, vector);
    case DUCKDB_TYPE_USMALLINT:
        return make_uint16s_from_vector(env, chunk_size, vector);

    case DUCKDB_TYPE_HUGEINT:
    {
        ERL_NIF_TERM i128s = make_int128s_from_vector(env, chunk_size, vector);
        return enif_make_tuple2(env, enif_make_int(env, DUCKDB_TYPE_HUGEINT), i128s);
    }
    case DUCKDB_TYPE_UHUGEINT:
    {
        ERL_NIF_TERM u128s = make_uint128s_from_vector(env, chunk_size, vector);
        return enif_make_tuple2(env, enif_make_int(env, DUCKDB_TYPE_UHUGEINT), u128s);
    }

    default:
        // TODO
        return make_badarg(env, enif_make_tuple2(env, enif_make_atom(env, "DUCKDB_TYPE"), enif_make_int(env, type)));
    }
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
duxdb_param_type(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    duckdb_type param_type = duckdb_param_type(stmt->duck, idx);
    if (param_type == DUCKDB_TYPE_INVALID)
        return make_badarg(env, argv[1]);

    return enif_make_int(env, param_type);
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

    // TODO refactor
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

    double f64;
    if (!enif_get_double(env, argv[2], &f64) || duckdb_bind_double(stmt->duck, idx, f64) == DuckDBError)
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

    ErlNifSInt64 i64;
    if (!enif_get_int64(env, argv[2], &i64) || duckdb_bind_int64(stmt->duck, idx, i64) == DuckDBError)
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

    ErlNifUInt64 u64;
    if (!enif_get_uint64(env, argv[2], &u64) || duckdb_bind_uint64(stmt->duck, idx, u64) == DuckDBError)
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
    if (!enif_inspect_binary(env, argv[2], &value) || duckdb_bind_varchar_length(stmt->duck, idx, (const char *)value.data, value.size) == DuckDBError)
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
    if (!enif_inspect_binary(env, argv[2], &value) || duckdb_bind_blob(stmt->duck, idx, (const char *)value.data, value.size) == DuckDBError)
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
    if (!enif_get_int64(env, argv[2], &days) || duckdb_bind_date(stmt->duck, idx, (duckdb_date){.days = days}) == DuckDBError)
        return make_badarg(env, argv[2]);

    return am_ok;
}

static ERL_NIF_TERM
duxdb_bind_time(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    ErlNifSInt64 micros;
    if (!enif_get_int64(env, argv[2], &micros) || duckdb_bind_time(stmt->duck, idx, (duckdb_time){.micros = micros}) == DuckDBError)
        return make_badarg(env, argv[2]);

    return am_ok;
}

static ERL_NIF_TERM
duxdb_bind_timestamp(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    ErlNifSInt64 micros;
    if (!enif_get_int64(env, argv[2], &micros) || duckdb_bind_timestamp(stmt->duck, idx, (duckdb_timestamp){.micros = micros}) == DuckDBError)
        return make_badarg(env, argv[2]);

    return am_ok;
}

static ERL_NIF_TERM
duxdb_bind_hugeint(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    ErlNifSInt64 upper;
    ErlNifUInt64 lower;

    if (!enif_get_int64(env, argv[2], &upper) ||
        !enif_get_uint64(env, argv[3], &lower) ||
        duckdb_bind_hugeint(stmt->duck, idx, (duckdb_hugeint){.lower = lower, .upper = upper}) == DuckDBError)
        return am_error;

    return am_ok;
}

static ERL_NIF_TERM
duxdb_bind_uhugeint(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx))
        return make_badarg(env, argv[1]);

    ErlNifUInt64 upper;
    ErlNifUInt64 lower;

    if (!enif_get_uint64(env, argv[2], &upper) ||
        !enif_get_uint64(env, argv[3], &lower) ||
        duckdb_bind_uhugeint(stmt->duck, idx, (duckdb_uhugeint){.lower = lower, .upper = upper}) == DuckDBError)
        return am_error;

    return am_ok;
}

static ERL_NIF_TERM
duxdb_bind_null(ErlNifEnv *env, int argc, const ERL_NIF_TERM argv[])
{
    duxdb_stmt *stmt;
    if (!enif_get_resource(env, argv[0], stmt_t, (void **)&stmt) || !(stmt->duck))
        return make_badarg(env, argv[0]);

    ErlNifUInt64 idx;
    if (!enif_get_uint64(env, argv[1], &idx) || duckdb_bind_null(stmt->duck, idx) == DuckDBError)
        return make_badarg(env, argv[1]);

    return am_ok;
}

static ErlNifFunc nif_funcs[] = {
    {"library_version", 0, duxdb_library_version, 0},

    {"config_count", 0, duxdb_config_count, 0},
    {"get_config_flag", 1, duxdb_get_config_flag, 0},

    // The create_config function is typically called first and can take a
    // significant amount of time on the initial call, likely due to the library
    // initialization or resource loading it triggers. For this reason, it is
    // scheduled as a CPU-bound dirty job to avoid blocking the main Erlang scheduler.
    {"create_config", 0, duxdb_create_config, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"set_config", 3, duxdb_set_config, ERL_NIF_DIRTY_JOB_CPU_BOUND},
    {"destroy_config", 1, duxdb_destroy_config, ERL_NIF_DIRTY_JOB_CPU_BOUND},

    {"open_ext", 2, duxdb_open_ext, ERL_NIF_DIRTY_JOB_IO_BOUND},
    {"close", 1, duxdb_close, ERL_NIF_DIRTY_JOB_IO_BOUND},

    {"connect", 1, duxdb_connect, 0},
    {"interrupt", 1, duxdb_interrupt, 0},
    {"query_progress", 1, duxdb_query_progress, 0},
    {"disconnect", 1, duxdb_disconnect, 0},

    // TODO remove, leave dirty only
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
    // TODO destoy once empty
    {"destroy_data_chunk", 1, duxdb_destroy_data_chunk, 0},
    {"data_chunk_get_column_count", 1, duxdb_data_chunk_get_column_count, 0},
    {"data_chunk_get_vector_nif", 2, duxdb_data_chunk_get_vector, 0},

    {"prepare_nif", 2, duxdb_prepare_nif, ERL_NIF_DIRTY_JOB_CPU_BOUND},

    {"destroy_prepare", 1, duxdb_destroy_prepare, 0},
    {"nparams", 1, duxdb_nparams, 0},
    {"parameter_name", 2, duxdb_parameter_name, 0},
    {"param_type", 2, duxdb_param_type, 0},
    {"bind_parameter_index_nif", 2, duxdb_bind_parameter_index, 0},
    {"clear_bindings", 1, duxdb_clear_bindings, 0},
    {"prepared_statement_type", 1, duxdb_prepared_statement_type, 0},

    // TODO remove, leave dirty only
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
    {"bind_time_nif", 3, duxdb_bind_time, 0},
    {"bind_timestamp_nif", 3, duxdb_bind_timestamp, 0},
    {"bind_hugeint_nif", 4, duxdb_bind_hugeint, 0},
    {"bind_uhugeint_nif", 4, duxdb_bind_uhugeint, 0},
    {"bind_null", 2, duxdb_bind_null, 0},
};

ERL_NIF_INIT(Elixir.DuxDB, nif_funcs, on_load, NULL, NULL, NULL)
