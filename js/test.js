const FlatfileWriter = require('./run').FlatfileWriter;
const { column_get, column_difference } = require('./iter');

async function main() {
  let f = new FlatfileWriter("/tmp/_test1.dat", [
      [ "s", "string", false ],
      [ "i64", "u64", false ],
      [ "i32", "u32", false ],
  ]);
  for (let i = 0; i < 10 ; ++i) {
      await f.pwrite([String(i), i * i, i]);
//      console.log(i);
  }
  f.close();

  let h = new FlatfileWriter("/tmp/_test2.dat", [
      [ "s", "string", false ],
      [ "i64", "u64", false ],
      [ "i32", "u32", false ],
  ]);
  for (let i = 10; i < 20 ; ++i) {
      await h.pwrite([String(i), i * i, i]);
  }
  h.close()

  let g = new FlatfileWriter("_test_odd.dat", [[ "odd", "u32", false ]]);
  for (let i = 1; i < 10; i += 2) {
    await g.pwrite([i]);
  }
  g.close();
}

async function iter_test() {
//  column_get("_test.dat", "i32", err => console.log('err'), v => console.log('i', v), () => console.log('complete_'));
//  column_get("_test_odd.dat", "odd", err => console.log('err'), v => console.log('o', v), () => console.log('complete_'));
  column_difference("_test.dat", "i32", "_test_odd.dat", "odd", err => console.log("err", err), v => console.log("diff", v), () => console.log('diff done'));
}


main()
  .then(x => iter_test())

