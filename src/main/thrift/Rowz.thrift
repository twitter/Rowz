namespace java com.twitter.rowz.thrift
namespace rb Rowz

struct Row {
  1: i64 id
  2: string name
  3: i64 created_at
  4: i64 updated_at
}

exception RowzException {
  1: string description
}

service Rowz {
  i64 create(1: string name) throws(1: RowzException ex)
  void update(1: Row row) throws(1: RowzException ex)
  void destroy(1: i64 id) throws(1: RowzException ex)
  Row read(1: i64 id) throws(1: RowzException ex)
}
