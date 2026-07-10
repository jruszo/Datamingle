# Datamingle Agent Package

Unpack this archive so `bin/` and `data/` share the same agent base directory.
Set `data_dir` in `config/agent.yaml` to that `data/` directory.

The package includes gh-ost, pt-online-schema-change, and pt-archiver. The two
Percona tools are Perl programs and require DBI plus the MySQL DBI driver on the
agent host. On Debian or Ubuntu, install them with:

```bash
sudo apt-get install perl libdbi-perl libdbd-mysql-perl
```
