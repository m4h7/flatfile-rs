{
    "targets": [{
        "target_name": "module",
        "include_dirs": [ "../include" ],
        "sources": [ "./module.c" ],
        "link_settings": {
            "libraries": [
                "-L<(module_root_dir)/../clib/target/debug/",
                "-llibflatfile"
            ]
        }
    }]
}
