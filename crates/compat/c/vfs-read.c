/*
 * Reads a database the way an application on the Go read-replica VFS does, for the compat
 * harness (crates/compat/src/vfs.rs). Upstream ships the VFS as a SQLite loadable extension
 * and its own end-to-end test drives it from the sqlite3 shell; this is the same host with
 * output the harness can compare.
 *
 *   vfs-read <uri> <pages-out> [extension]
 *
 * With an extension, it is loaded first (entry point sqlite3_litestreamvfs_init) and <uri>
 * names the database on it (file:app.db?vfs=litestream, the replica from
 * LITESTREAM_REPLICA_URL). Without one, <uri> is an ordinary file, read the same way, so a
 * reference database prints in the same format.
 *
 * Output on stdout, one record per line, tab separated:
 *   integrity <row of PRAGMA integrity_check>
 *   txid <litestream_txid()>                 (with an extension only)
 *   row <table> <cell>,<cell>,...            (every row of every table, in scan order)
 * A cell is n (NULL), i<decimal>, f<C99 hex float>, t<hex of UTF-8> or b<hex>.
 * <pages-out> receives every page exactly as the VFS's xRead returns it, read inside one
 * read transaction. Any failure prints "error <what>" on stderr and exits 1.
 */
#include <sqlite3.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

static int fail(const char *what, sqlite3 *db) {
  fprintf(stderr, "error\t%s: %s\n", what, db ? sqlite3_errmsg(db) : "");
  return 1;
}

static void hex(const unsigned char *p, int n) {
  for (int i = 0; i < n; i++) printf("%02x", p[i]);
}

static void cell(sqlite3_stmt *st, int i) {
  switch (sqlite3_column_type(st, i)) {
  case SQLITE_NULL: printf("n"); break;
  case SQLITE_INTEGER: printf("i%lld", (long long)sqlite3_column_int64(st, i)); break;
  case SQLITE_FLOAT: printf("f%a", sqlite3_column_double(st, i)); break;
  case SQLITE_TEXT:
    printf("t");
    hex(sqlite3_column_text(st, i), sqlite3_column_bytes(st, i));
    break;
  default:
    printf("b");
    hex(sqlite3_column_blob(st, i), sqlite3_column_bytes(st, i));
    break;
  }
}

/* Prints every row of every statement `sql` yields, each as one record of kind `kind`. */
static int each(sqlite3 *db, const char *sql, const char *kind, const char *table) {
  sqlite3_stmt *st;
  if (sqlite3_prepare_v2(db, sql, -1, &st, 0) != SQLITE_OK) return fail(sql, db);
  int rc;
  while ((rc = sqlite3_step(st)) == SQLITE_ROW) {
    printf("%s\t", kind);
    if (table) printf("%s\t", table);
    for (int i = 0; i < sqlite3_column_count(st); i++) {
      if (i) printf(",");
      if (table) cell(st, i); else printf("%s", (const char *)sqlite3_column_text(st, i));
    }
    printf("\n");
  }
  sqlite3_finalize(st);
  return rc == SQLITE_DONE ? 0 : fail(sql, db);
}

static int tables(sqlite3 *db) {
  sqlite3_stmt *st;
  const char *sql = "SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name";
  if (sqlite3_prepare_v2(db, sql, -1, &st, 0) != SQLITE_OK) return fail(sql, db);
  int rc, bad = 0;
  while (!bad && (rc = sqlite3_step(st)) == SQLITE_ROW) {
    const char *name = (const char *)sqlite3_column_text(st, 0);
    char *q = sqlite3_mprintf("SELECT * FROM \"%w\"", name);
    bad = each(db, q, "row", name);
    sqlite3_free(q);
  }
  sqlite3_finalize(st);
  return bad ? 1 : (rc == SQLITE_DONE ? 0 : fail(sql, db));
}

static int pages(sqlite3 *db, const char *out) {
  sqlite3_file *file = 0;
  sqlite3_int64 size = 0;
  int page = 0;
  sqlite3_stmt *st;
  if (sqlite3_exec(db, "BEGIN; SELECT count(*) FROM sqlite_schema", 0, 0, 0) != SQLITE_OK)
    return fail("begin a read", db);
  if (sqlite3_prepare_v2(db, "PRAGMA page_size", -1, &st, 0) != SQLITE_OK ||
      sqlite3_step(st) != SQLITE_ROW)
    return fail("page_size", db);
  page = sqlite3_column_int(st, 0);
  sqlite3_finalize(st);
  if (sqlite3_file_control(db, "main", SQLITE_FCNTL_FILE_POINTER, &file) != SQLITE_OK || !file ||
      !file->pMethods)
    return fail("file pointer", db);
  if (file->pMethods->xFileSize(file, &size) != SQLITE_OK) return fail("xFileSize", db);
  FILE *f = fopen(out, "wb");
  unsigned char *buf = malloc(page);
  if (!f || !buf) return fail("open the pages file", 0);
  for (sqlite3_int64 off = 0; off < size; off += page) {
    if (file->pMethods->xRead(file, buf, page, off) != SQLITE_OK) return fail("xRead", db);
    if (fwrite(buf, 1, page, f) != (size_t)page) return fail("write the pages file", 0);
  }
  free(buf);
  if (fclose(f) != 0) return fail("close the pages file", 0);
  return sqlite3_exec(db, "COMMIT", 0, 0, 0) == SQLITE_OK ? 0 : fail("end the read", db);
}

int main(int argc, char **argv) {
  if (argc < 3 || argc > 4) {
    fprintf(stderr, "usage: vfs-read <uri> <pages-out> [extension]\n");
    return 2;
  }
  sqlite3 *host = 0, *db = 0;
  if (argc == 4) {
    char *err = 0;
    if (sqlite3_open(":memory:", &host) != SQLITE_OK) return fail("open the host", host);
    sqlite3_enable_load_extension(host, 1);
    if (sqlite3_load_extension(host, argv[3], "sqlite3_litestreamvfs_init", &err) != SQLITE_OK) {
      fprintf(stderr, "error\tload %s: %s\n", argv[3], err ? err : "");
      return 1;
    }
  }
  int flags = SQLITE_OPEN_READONLY | SQLITE_OPEN_URI;
  if (sqlite3_open_v2(argv[1], &db, flags, 0) != SQLITE_OK) return fail("open", db);
  if (each(db, "PRAGMA integrity_check", "integrity", 0)) return 1;
  if (host && each(db, "SELECT litestream_txid()", "txid", 0)) return 1;
  if (tables(db) || pages(db, argv[2])) return 1;
  if (fflush(stdout) != 0) return fail("flush", 0);
  sqlite3_close(db);
  return 0;
}
