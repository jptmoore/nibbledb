type t;

let create: unit => t;

let push: (t, (int64, Ezjsonm.t)) => unit;

let pop: t => (int64, Ezjsonm.t);

let length: t => int;

let to_list: t => list((int64, Ezjsonm.t));

let is_ascending: (t, int64) => bool;

let is_descending: (t, int64) => bool;

let clear: t => unit;

let set_disk_range: (t, option((int64, int64))) => unit;

let get_disk_range: t => option((int64, int64));