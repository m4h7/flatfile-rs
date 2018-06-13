const FlatfileWriter = require('./run').FlatfileWriter;

async function main() {
  let f = new FlatfileWriter("_test.dat", [
      [ "s", "string", false ],
      [ "i64", "u64", false ],
      [ "i32", "u32", false ],
  ]);
  for (let i = 0; i < 10000 ; ++i) {
      await f.pwrite([String(i), i * i, i]);
      console.log(i);
  }
  f.close();
}


main()

