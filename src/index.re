open Lwt.Infix;

type index = list((int64,int64));
  
let index_t = Irmin.Type.(list(pair(int64, int64)));
  
let print = (m) => Fmt.pr("%a\n%!", Irmin.Type.pp_json(index_t), m);
  
let merge = Irmin.Merge.(option(idempotent(index_t)));

module Index: Irmin.Contents.S with type t = index = {
  type t = index;
  let t = index_t;
  let merge = merge;
  /* let pp = Irmin.Type.pp_json(index_t); */
  /* let of_string = (s) => Irmin.Type.decode_json(index_t, Jsonm.decoder(`String(s))); */
};

module Store = Irmin_unix.Git.FS.KV(Index);

type t = Store.t;

let filter_list = (rem_lis, lis) => {
  open List;
  let rec loop = (acc, l) =>
    switch l {
    | [] => acc
    | [x, ...xs] => mem(x, rem_lis) ? loop(acc, xs) : loop(cons(x, acc), xs)
    };
  loop([], lis);
};

let bounds = (lis) => {
  if (lis == []) {
      None;
    } else {  
      Some(List.fold_left(((x,y), (x',y')) => 
        (min(x,x'),max(y,y')), (Int64.max_int,Int64.min_int), lis))
    }
}

let tup_sort = (lis) => {
  let cmp = ((_, y), (_, y')) => y < y' ? 1 : (-1);
  List.sort(cmp, lis);
};

let add_tuple = (tup, lis) => List.cons(tup, lis) |> tup_sort;


let create = (~file, ~bare) => {
  let config = Irmin_git.config(file, ~bare);
  let repo = Store.Repo.v(config);
  repo >>= (repo => Store.master(repo));
};

let read = (branch, k) => {
  branch >>= (branch' => Store.find(branch', [k]));
};

let write = (branch, info, k, v) => {
  branch >>= (branch' => Store.set_exn(branch', ~info, [k], v));
};

let update = (branch, info, k, tup, remove_list) => {
  read(branch, k) >>=
    (data) => 
      switch data {
      | Some((curr_lis)) => {
          let filtered = filter_list(remove_list, curr_lis);
          let new_index = add_tuple(tup, filtered);
          write(branch, info, k, new_index) >|=
            () => bounds(new_index)
        };
      | None => write(branch, info, k, [tup]) >|=
          () => bounds([tup])
      };
};

let get = (branch, k) => {
  read(branch, k)
};

let length = (branch, k) => {
  read(branch, k) >|=
    (data) =>
      switch data {
      | None => 0;
      | Some(index) => List.length(index);
      }
}

let overlap_worker = (index, lis) => {
  let (x, y) = index;
  List.filter(((x', y')) => x <= y' && y >= x', lis);
};

let overlap = (branch, k, index) => {
  read(branch, k) >|=
      (data) => {
        switch data {
        | Some((lis)) => overlap_worker(index, lis)
        | None => []
        }
      };
};

let range = (branch, k) => {
  read(branch, k) >|=
    (data) => {
      switch data {
      | None => None
      | Some((lis)) => bounds(lis)
      }
    };
};




