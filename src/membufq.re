
type t = {
  q: Queue.t((int64, Ezjsonm.t)),
  mutable disk_range: option((int64, int64))
};

let create = () => {q: Queue.create(), disk_range: None};

let push = (ctx, n) => Queue.push(n, ctx.q);

let pop = (ctx) => Queue.pop(ctx.q);

let length = (ctx) => Queue.length(ctx.q);

let to_list = (ctx) => Queue.fold((x, y) => List.cons(y, x), [], ctx.q);

let is_ascending = (ctx, ub) => {
  let rec is_sorted = (lis) =>
    switch lis {
    | [(t1,_), (t2,j2), ...l] =>
      t1 >= t2 && is_sorted([(t2, j2), ...l])
    | _ => true
    };
  switch (to_list(ctx)) {
  | [] => true
  | [(t,j), ...l] => is_sorted([(t, j), ...l]) && t >= ub
  };
};

let is_descending = (ctx, lb) => {
  let rec is_sorted = (lis) =>
    switch lis {
    | [(t1,_), (t2,j2), ...l] =>
      t1 <= t2 && is_sorted([(t2, j2), ...l])
    | _ => true
    };
  switch (to_list(ctx)) {
  | [] => true
  | [(t,j), ...l] => is_sorted([(t, j), ...l]) && t <= lb
  };
};

let clear = (ctx) => Queue.clear(ctx.q);

let set_disk_range = (ctx, range) => ctx.disk_range = range;

let get_disk_range = (ctx) => ctx.disk_range;