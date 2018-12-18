open Lwt.Infix;

type t = {
  membuf: Membuf.t,
  index: Lwt.t(Index.t),
  shard: Lwt.t(Shard.t),
  max_buffer_size: int,
  shard_size: int
};

let create = (~path_to_db, ~max_buffer_size, ~shard_size, ~show_files) => {
  membuf: Membuf.create(),
  index: Index.create(~file=path_to_db ++ "_index_store", ~bare=!show_files),
  shard: Shard.create(~file=path_to_db ++ "_shard_store", ~bare=!show_files),
  max_buffer_size: max_buffer_size,
  shard_size: shard_size
};


let get_milliseconds = () => {
  let t = Unix.gettimeofday() *. 1000.0;
  Int64.of_float(t);
};

let get_nanoseconds = () => {
  let t = Unix.gettimeofday() *. 1000.0 *. 1000.0 *. 1000.0;
  Int64.of_float(t);
};

let get_microseconds = () => {
  let t = Unix.gettimeofday() *. 1000.0 *. 1000.0;
  Int64.of_float(t);
};

/* let get_milliseconds_ptime = () => {
  open Int64;
  let (days, picoseconds) = Ptime_clock.now_d_ps();
  let seconds_from_days = days * 86400;
  let milliseconds_from_seconds = of_int(seconds_from_days * 1000);
  let milliseconds_from_picoseconds = div(picoseconds, of_int(1000 * 1000 * 1000));
  add(milliseconds_from_seconds, milliseconds_from_picoseconds);
};

let get_nanoseconds_ptime = () => {
  open Int64;
  let (days, picoseconds) = Ptime_clock.now_d_ps();
  let seconds_from_days = days * 86400;
  let nanoseconds_from_seconds = of_int(seconds_from_days * 1000 * 1000 * 1000);
  let nanoseconds_from_picoseconds = div(picoseconds, of_int(1000));
  add(nanoseconds_from_seconds, nanoseconds_from_picoseconds);
}; */

let validate_json = (json) => {
  open Ezjsonm;
  open Int64;
  switch (get_dict(value(json))) {
  | [("value",`Float n)] => 
      Some((get_microseconds(), json));
  | [("timestamp",`Float ts), ("value",`Float n)] => 
      Some((of_float(ts), dict([("value",`Float(n))])));
  | [(tag_name, `String tag_value), ("value",`Float n)] => 
      Some((get_microseconds(), json));
  | [("timestamp",`Float ts), (tag_name, `String tag_value), ("value",`Float n)] => 
      Some((of_float(ts), dict([(tag_name, `String(tag_value)), ("value",`Float(n))])));
  | _ => None;
  }
};

let shard_range = lis => {
  open List;
  let cmp = (x, y) => x > y ? 1 : (-1);
  switch (lis) {
  | [] => None
  | _ =>
    Some(
      map(((ts, _)) => ts, lis)
      |> sort(cmp)
      |> (lis' => (hd(lis'), hd(rev(lis')))),
    )
  };
};

let make_key = (id, (t1, t2)) => [
  id,
  Int64.to_string(t1),
  Int64.to_string(t2),
];

let shard_data = (ctx, id) => {
  let rec loop = (n, shard) =>
    if (n > 0) {
      Membuf.read(ctx.membuf, id)
      >>= (elt => loop(n - 1, List.cons(elt, shard)));
    } else {
      Lwt.return(shard);
    };
  loop(ctx.shard_size, []);
};



let string_of_key = lis => List.fold_left((x, y) => x ++ ":" ++ y, "", lis);

let log_index = (str, lis) => {
  Lwt_list.iter_s(((x, y)) => Lwt_log_core.debug_f("%s:(%Ld,%Ld)\n", str, x, y), lis);
};

let remove_leftover_shards = (ctx, k, keep_index, remove_list, info) => {
  open List;
  let index_list = filter(i => i != keep_index, remove_list);
  let key_list = map(i => make_key(k, i), index_list);
  Shard.remove(ctx.shard, info, key_list);
};

let handle_shard_overlap_worker = (ctx, k, shard, shard_lis, overlap_list, info) => {
  open List;
  let new_shard = flatten(cons(shard, shard_lis));
  Lwt_log_core.debug_f("shard len:%d", List.length(new_shard)) >>= 
    () => switch (shard_range(new_shard)) {
          | Some(new_range) =>
            let key = make_key(k, new_range);
            Lwt_log_core.debug_f("Adding shard with key%s", string_of_key(key)) >>= 
              () => Index.update(ctx.index, info, k, new_range, overlap_list) >>= 
                bounds => Membuf.set_disk_range(ctx.membuf, k, bounds) |> 
                  () => Shard.add(ctx.shard, info, key, new_shard) >>= 
                    () => remove_leftover_shards(ctx, k, new_range, overlap_list, info);
          | None => Lwt.return_unit;
        };
};

let handle_shard_overlap = (ctx, k, shard, range, info) => {
  Index.overlap(ctx.index, k, range) >>= 
    overlap_list => log_index("overlapping shards", overlap_list) >>= 
      () => Lwt_list.map_s(r => Shard.get(ctx.shard, make_key(k, r)), overlap_list) >>= 
        shard_list => handle_shard_overlap_worker(ctx, k, shard, shard_list, overlap_list, info)
};
          
      

let handle_shard = (ctx, k, shard, info) => {
  switch (shard_range(shard)) {
  | Some(range) => handle_shard_overlap(ctx, k, shard, range, info)
  | None => Lwt.return_unit
  };
};

let write = (~ctx, ~info, ~timestamp as t, ~id as k, ~json as j) => {
  Membuf.write(ctx.membuf, k, (t, j)) >>= 
    () => Membuf.length(ctx.membuf, k) >>=
      current_buffer_size =>
        if (current_buffer_size == ctx.max_buffer_size) {
          shard_data(ctx, k) >>= 
            (data => handle_shard(ctx, k, Shard.convert(data), info));
        } else {
            Lwt.return_unit;
        }
};

let flush_series = (ctx, k, shard, info) => {
  handle_shard(ctx, k, Shard.convert(shard), info) >>= 
    () => Membuf.empty_series(ctx.membuf, k);
};

let flush = (~ctx, ~info) => {
  Membuf.serialise(ctx.membuf) >>= 
    lis => Lwt_list.iter_s(((key, shard)) => 
      flush_series(ctx, key, shard, info), lis);
};

let number_of_records_on_disk = (ctx, k, lis) => {
  Lwt_list.fold_left_s( (acc, x) => 
    Shard.get(ctx.shard, make_key(k, x)) >|= 
      (x => List.length(x) + acc), 0, lis);
};

let number_of_records_in_memory = (ctx, k) => {
  Membuf.(exists(ctx.membuf, k) ? length(ctx.membuf, k) : Lwt.return(0));
};

let length_worker = (~ctx, ~id as k) => {
  open Ezjsonm;
  Index.get(ctx.index, k) >>= 
    data => {
      switch (data) {
      | Some(lis) => number_of_records_on_disk(ctx, k, lis)
      | None => 0 |> Lwt.return
      } >>= disk => 
        number_of_records_in_memory(ctx, k) >>= 
          (mem => Lwt.return(disk + mem))
    };
};
    

let length = (~ctx, ~id_list) => {
  open Ezjsonm;
  Lwt_list.fold_left_s((acc, id) => length_worker(~ctx, ~id) >|= 
    (x => x + acc), 0, id_list)
};

let read_memory_all = (ctx, id) => {
  Membuf.exists(ctx.membuf, id) ?
    Membuf.to_list(ctx.membuf, id) : Lwt.return([]);
};

let flush_memory = (ctx, k, info) => {
  read_memory_all(ctx, k) >>= 
    (shard => flush_series(ctx, k, shard, info));
};

let flush_memory_worker = (ctx, id, info) => {
  Membuf.exists(ctx.membuf, id) ?
    flush_memory(ctx, id, info) : Lwt.return_unit;
};
   

let get_timestamps = json => {
  open Ezjsonm;
  List.rev_map(x => get_int64(x), get_list(x => find(x, ["timestamp"]), json));
};

let filter_shard_worker = (ctx, key, timestamps, info) => {
  Shard.get(ctx.shard, key) >>=
    lis => List.filter(((t, _)) => ! List.mem(t, timestamps), lis) |> 
      (lis' => Shard.add(ctx.shard, info, key, lis'))
};

let delete_worker = (ctx, key_list, timestamps, info) => {
  Lwt_list.iter_s(k => filter_shard_worker(ctx, k, timestamps, info), key_list);
};

let make_shard_keys_worker = (id, lb, lis) => {
  let rec loop = (acc, lis) =>
    switch (lis) {
    | [] => acc
    | [(t1, t2), ...rest] when lb > t2 => loop(acc, rest)
    | [(t1, t2), ...rest] => loop(List.cons(make_key(id, (t1, t2)), acc), rest)
    };
  loop([], lis);
};

let make_shard_keys = (ctx, id, lb) => {
  Index.get(ctx.index, id) >>= 
    lis => {
      switch (lis) {
      | None => []
      | Some(lis') => make_shard_keys_worker(id, lb, lis')
      } |> Lwt.return
    };
};

let delete = (~ctx, ~info, ~id_list, ~json) => {
  let timestamps = get_timestamps(Ezjsonm.value(json));
  switch (timestamps) {
  | [] => Lwt.return_unit
  | [lb, ..._] =>
      Lwt_list.iter_s(id => flush_memory_worker(ctx, id, info), id_list) >>= 
        () => Lwt_list.map_s(k => make_shard_keys(ctx, k, lb), id_list) >>= 
          keys' => Lwt_list.iter_s(k => delete_worker(ctx, k, timestamps, info), keys')
  };
};

let take = (n, lis) => {
  open List;
  let rec loop = (n, acc, l) =>
    switch (l) {
    | [] => acc
    | [xs, ...rest] when n == 0 => acc
    | [xs, ...rest] => loop(n - 1, cons(xs, acc), rest)
    };
  rev(loop(n, [], lis));
};

let sort_result = (mode, lis) => {
  open List;
  switch (mode) {
  | `Last => fast_sort(((x, y), (x', y')) => x < x' ? 1 : (-1), lis)
  | `First => fast_sort(((x, y), (x', y')) => x > x' ? 1 : (-1), lis)
  | `None => lis
  };
};


let return_data = (~sort as mode, lis) => {
  sort_result(mode, lis) |> Shard.to_json |> Lwt.return;
};
 

let read_disk = (ctx, k, n, mode) => {
  open List;
  let rec loop = (n, acc, lis) =>
    switch (lis) {
    | [] => acc |> Lwt.return
    | [tup, ...rest] => Shard.get(ctx.shard, make_key(k, tup)) >>= 
        shard => {
          let leftover = n - length(shard);
          if (leftover > 0) {
            loop(leftover, rev_append(shard, acc), rest);
          } else {
            rev_append(take(n, sort_result(mode, shard)), acc) |> Lwt.return;
          };
        }
    };
    Lwt_log_core.debug_f("read_disk\n") >>= 
      () => Index.get(ctx.index, k) >>= 
        data => {
            switch (mode, data) {
            | (`Last, Some(lis)) => lis
            | (`First, Some(lis)) => rev(lis)
            | (_, None) => []
            } |> loop(n, [])
        };
};

let flush_memory_read_from_disk = (ctx, k, n, mode, info) => {
  Lwt_log_core.debug_f("flush_memory_read_from_disk\n") >>= 
    (() => flush_memory(ctx, k, info) >>= 
      (() => read_disk(ctx, k, n, mode)));
};

let is_ascending = (ctx, k) => {
  Membuf.get_disk_range(ctx.membuf, k) |> 
    (range) => {
      switch range {
      | None => false
      | Some(((lb, ub))) => Membuf.is_ascending(ctx.membuf, k, ub)
      }
    };
};


let is_descending = (ctx, k) => {
  Membuf.get_disk_range(ctx.membuf, k) |> 
    (range) => {
      switch range {
      | None => false
      | Some(((lb, ub))) => Membuf.is_descending(ctx.membuf, k, lb)
      }
    };
};

let take_from_memory = (n, lis, mode) => {
    open List;
    let count = min(n, length(lis));
    let sorted = sort_result(mode, lis);
    (n - count, take(count, sorted)) |> Lwt.return;
};
  
let read_memory = (ctx, id, n, mode) => {
    Lwt_log_core.debug_f("read_memory\n") >>= 
      () => Membuf.to_list(ctx.membuf, id) >>= 
        ((mem_shard) => take_from_memory(n, mem_shard, mode))
};

let read_disk = (ctx, k, n, mode) => {
    open List;
    let rec loop = (n, acc, lis) => {
      switch lis {
      | [] => acc |> Lwt.return
      | [tup, ...rest] => Shard.get(ctx.shard, make_key(k, tup)) >>= 
          (shard) => {
            let leftover = n - length(shard);
            if (leftover > 0) {
              loop(leftover, rev_append(shard, acc), rest);
            } else {
              rev_append(take(n, sort_result(mode, shard)), acc) |> Lwt.return;
            };
          }
      };
    };
    Lwt_log_core.debug_f("read_disk\n") >>= 
      () => Index.get(ctx.index, k) >>= 
        data => { 
            switch (mode, data) {
            | (`Last, Some((lis))) => lis
            | (`First, Some((lis))) => rev(lis)
            | (_, None) => []
            } |> loop(n, []) 
        };   
  };

let read_memory_then_disk = (ctx, k, n, mode) => {
  Lwt_log_core.debug_f("read_memory_then_disk\n") >>= 
    () => read_memory(ctx, k, n, mode) >>= 
      ((leftover, mem)) => 
        if (leftover > 0) {
          read_disk(ctx, k, leftover, mode)
            >|= (disk => List.rev_append(Shard.convert(mem), disk));
        } else {
          Shard.convert(mem) |> Lwt.return;
        }
};

let aggregate = (data, name, func) => {
  open Ezjsonm;
  Shard.values(data) |> Array.of_list |> func |>
    result => dict([(name, `Float(result))]);
};

let count = (data) => {
  let count = float_of_int(List.length(data));
  Ezjsonm.dict([("count", `Float(count))]);
};  

let return_aggregate_data = (data, arg) => {
  open Oml.Util.Array;
  open Oml.Statistics.Descriptive;
  switch (arg) {
  | ["sum"] => aggregate(data, "sum", sumf);
  | ["max"] => aggregate(data, "max", max);
  | ["min"] => aggregate(data, "min", min);
  | ["mean"] => aggregate(data, "mean", mean);
  | ["sd"] => aggregate(data, "sd", sd);
  | ["median"] => aggregate(data, "median", median);
  | ["count"] => count(data)
  | _ => failwith("Error:unknown path\n")
  } |> Lwt.return;
};

let read_last_worker = (~ctx, ~id as k, ~n, ~info) => {
  if (Membuf.exists(ctx.membuf, k)) {
    is_ascending(ctx, k) ?
      read_memory_then_disk(ctx, k, n, `Last) : flush_memory_read_from_disk(ctx, k, n, `Last, info);
  } else {
    read_disk(ctx, k, n, `Last);
  };
};

let return_filtered_data = (~sort, ~tag, data, func) => {
  let (name, value) = tag;
  Shard.filter(data, func, (name,value)) |> 
    data' => return_data(~sort, data')
};

let return_filtered_aggregate_data = (~sort, ~tag, data, func, agg_mode) => {
  let (name, value) = tag;
  Shard.filter(data, func, (name,value)) |> 
    data' => return_aggregate_data(data', [agg_mode]);
};

module String_extra = {
  let contains = (s1, s2) => {
    let re = Str.regexp_string(s1);
    try {
      ignore(Str.search_forward(re, s2, 0));
      true;
    } {
    | Not_found => false
    };
  };
};

let process_data = (data, args, ~sort) => {
  switch (args) {
    | [] => return_data(~sort, data)
    | ["filter", name, "equals", value] => return_filtered_data(~sort, ~tag=(name,value), data, String.equal)
    | ["filter", name, "equals", value, agg_mode] => return_filtered_aggregate_data(~sort, ~tag=(name,value), data, String_extra.contains, agg_mode)
    | ["filter", name, "contains", value] => return_filtered_data(~sort, ~tag=(name,value), data, String_extra.contains)
    | ["filter", name, "contains", value, agg_mode] => return_filtered_aggregate_data(~sort, ~tag=(name,value), data, String_extra.contains, agg_mode)
    | _ => return_aggregate_data(data, args)
    }
};

let read_last = (~ctx, ~info, ~id_list, ~n, ~xargs) => {
  Lwt_list.fold_left_s((acc, id) =>
    read_last_worker(~ctx=ctx, ~id=id, ~n=n, ~info=info) >|= 
      (x => List.rev_append(x, acc)), [], id_list)
        >>= data => process_data(data, xargs, ~sort=`Last)
};


let read_latest = (~ctx, ~info, ~id_list, ~xargs) => {
  read_last(~ctx=ctx, ~info=info, ~id_list=id_list, ~n=1, ~xargs);
};

let read_first_worker = (~ctx, ~id as k, ~n, ~info) => {
  if (Membuf.exists(ctx.membuf, k)) {
    is_descending(ctx, k) ?
      read_memory_then_disk(ctx, k, n, `First) :
      flush_memory_read_from_disk(ctx, k, n, `First, info);
  } else {
    read_disk(ctx, k, n, `First);
  };
};

let read_first = (~ctx, ~info, ~id_list, ~n, ~xargs) => {
  Lwt_list.fold_left_s((acc, id) =>
    read_first_worker(~ctx=ctx, ~id=id, ~n=n, ~info=info) >|= 
      (x => List.rev_append(x, acc)), [], id_list) >>= 
        (data => process_data(data, xargs, ~sort=`First));
};

let read_earliest = (~ctx, ~info, ~id_list, ~xargs) => {
  read_first(~ctx=ctx, ~info=info, ~id_list=id_list, ~n=1, ~xargs);
};


let make_filter_elt = (k, tup, mode) => {
  (mode, make_key(k, tup));
};

let filter_since = (ts, lis) => {
  List.filter(((t, _)) => t >= ts, lis);
};

let read_since_disk_worker = (ctx, k, ts, status) => {
  switch status {
  | `Complete => Shard.get(ctx.shard, k)
  | `Partial => Shard.get(ctx.shard, k) >|= 
      (shard => filter_since(ts, shard))
  };
};

let handle_read_since_disk = (ctx, ts, lis) => {
  Lwt_list.fold_left_s((acc, (status, key)) =>
    read_since_disk_worker(ctx, key, ts, status) >|=
      (x => List.rev_append(x, acc)), [], lis);
};

let read_since_disk = (ctx, k, ts) => {
  open List;
  let rec loop = (acc, lis) => {
    switch lis {
    | [] => acc
    | [(lb,ub), ...rest] when lb >= ts && ub >= ts =>
      loop(cons(make_filter_elt(k, (lb, ub), `Complete), acc), rest)
    | [(lb,ub), ...rest] when ub >= ts =>
      cons(make_filter_elt(k, (lb, ub), `Partial), acc)
    | [_, ...rest] => loop(acc, rest)
    };
  };
  Index.get(ctx.index, k) >>=
    (data) => {
      switch data {
      | Some((lis)) => lis
      | None => []
      }
    } |> loop([]) |> handle_read_since_disk(ctx, ts)
};

let read_since_memory = (ctx, k, ts) => {
  read_memory_all(ctx, k) >|=
    (data => filter_since(ts, data));
};

let read_since_worker = (~ctx, ~id as k, ~from as ts) => {
  read_since_memory(ctx, k, ts) >>=
    (mem) => read_since_disk(ctx, k, ts) >|= 
      ((disk) => List.rev_append(Shard.convert(mem), disk))
};

let read_since = (~ctx, ~info, ~id_list, ~from as ts, ~xargs) => {
  Lwt_list.fold_left_s((acc, id) =>
    read_since_worker(~ctx=ctx, ~id=id, ~from=ts) >|= 
      (x => List.rev_append(x, acc)), [], id_list) >>= 
        (data => process_data(data, xargs, ~sort=`Last))
};

let filter_until = (ts, lis) => {
  List.filter(((t, _)) => t <= ts, lis);
};

let read_range_worker = (~ctx, ~id as k, ~from as t1, ~to_ as t2) => {
  read_since_memory(ctx, k, t1) >>=
    (mem) => read_since_disk(ctx, k, t1) >>= 
      ((disk) => List.rev_append(Shard.convert(mem), disk) |> 
        filter_until(t2) |> Lwt.return)
};

let read_range = (~ctx, ~info, ~id_list, ~from as t1, ~to_ as t2, ~xargs) => {
  Lwt_list.fold_left_s((acc, id) =>
    read_range_worker(~ctx=ctx, ~id=id, ~from=t1, ~to_=t2) >|= 
      (x => List.rev_append(x, acc)), [], id_list) >>= 
        (data => process_data(data, xargs, ~sort=`Last))
};