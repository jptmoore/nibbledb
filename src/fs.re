open Lwt.Infix;

type t = {
  index_dir: string,
  cmd: Lwt_process.command
};

let gitcmd = "git ls-tree --name-only master";

let create = (~index_dir) => {
  { 
    index_dir: index_dir,
    cmd: Lwt_process.shell("cd " ++ index_dir ++ ";" ++ gitcmd)
  }
};

let ts_names = (~ctx) => {
  Lwt_process.pread_lines(ctx.cmd) |> 
    Lwt_stream.to_list
}