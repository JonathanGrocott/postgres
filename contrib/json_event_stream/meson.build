# Copyright (c) 2022-2026, PostgreSQL Global Development Group

install_data(
  'json_event_stream.control',
  'json_event_stream--1.0--1.1.sql',
  'json_event_stream--1.0.sql',
  'json_event_stream--1.1.sql',
  kwargs: contrib_data_args,
)

install_data(
  'json_event_bench',
  'json_event_ci',
  'json_event_package',
  'json_event_remote_guard',
  'json_event_smoke',
  install_dir: dir_bin,
  install_mode: 'rwxr-xr-x',
)

json_event_httpd_sources = files(
  'json_event_httpd.c',
)

if host_system == 'windows'
  json_event_httpd_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'json_event_httpd',
    '--FILEDESC', 'json_event_httpd - HTTP ingestion for JSON event streams',])
endif

json_event_httpd = executable('json_event_httpd',
  json_event_httpd_sources,
  dependencies: [frontend_code, libpq],
  kwargs: default_bin_args,
)
contrib_targets += json_event_httpd

json_event_ctl_sources = files(
  'json_event_ctl.c',
)

if host_system == 'windows'
  json_event_ctl_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'json_event_ctl',
    '--FILEDESC', 'json_event_ctl - manage JSON event streams',])
endif

json_event_ctl = executable('json_event_ctl',
  json_event_ctl_sources,
  dependencies: [frontend_code, libpq],
  kwargs: default_bin_args,
)
contrib_targets += json_event_ctl

json_event_mqttd_sources = files(
  'json_event_mqttd.c',
)

if host_system == 'windows'
  json_event_mqttd_sources += rc_bin_gen.process(win32ver_rc, extra_args: [
    '--NAME', 'json_event_mqttd',
    '--FILEDESC', 'json_event_mqttd - MQTT ingestion for JSON event streams',])
endif

json_event_mqttd = executable('json_event_mqttd',
  json_event_mqttd_sources,
  dependencies: [frontend_code, libpq],
  kwargs: default_bin_args,
)
contrib_targets += json_event_mqttd

tests += {
  'name': 'json_event_stream',
  'sd': meson.current_source_dir(),
  'bd': meson.current_build_dir(),
  'regress': {
    'sql': [
      'json_event_stream_upgrade',
      'json_event_stream',
    ],
  },
}
