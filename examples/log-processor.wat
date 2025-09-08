(module
  (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
  (import "wasi_snapshot_preview1" "proc_exit" (func $proc_exit (param i32)))
  
  (memory (export "memory") 1)
  
  ;; Function to process logs
  ;; This is a simple example that just returns success
  (func $process_logs (param i32) (result i32)
    ;; For now, just return success (0)
    i32.const 0
  )
  
  ;; Export the function
  (export "process_logs" (func $process_logs))
) 