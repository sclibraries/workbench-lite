from pathlib import Path
import re
from typing import Dict


class ConfigError(ValueError):
    pass


def load_config(config_path: Path) -> Dict[str, object]:
    path = Path(config_path)
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")

    try:
        import yaml  # type: ignore
    except ImportError as exc:
        raise ConfigError("PyYAML is required; install the Workbench-lite requirements.") from exc

    class UniqueKeyLoader(yaml.SafeLoader):
        pass

    def unique_mapping(loader, node):
        result = {}
        for key_node, value_node in node.value:
            key = loader.construct_object(key_node, deep=True)
            if not isinstance(key, str) or key in result:
                raise ConfigError('YAML mapping keys must be unique strings.')
            result[key] = loader.construct_object(value_node, deep=True)
        return result

    UniqueKeyLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, unique_mapping)
    with path.open("r", encoding="utf-8") as handle:
        try:
            loaded = yaml.load(handle, Loader=UniqueKeyLoader)
            if loaded is None:
                loaded = {}
        except yaml.YAMLError as exc:
            raise ConfigError("Invalid YAML syntax; check the configuration file.") from exc
    if not isinstance(loaded, dict):
        raise ConfigError("Workbench-lite config must be a YAML mapping.")
    return _validate_config(loaded)


def _validate_config(config: Dict[str, object]) -> Dict[str, object]:
    # Validate consumed scalar types before any generation/upload side effects.
    for name in ("input_dir", "input_csv", "manifest_base_url", "cantaloupe_base_url",
                 "s3_private_bucket", "s3_public_bucket", "s3_prefix", "batch_id"):
        if name in config and not isinstance(config[name], str):
            raise ConfigError(f"Configuration {name} must be a string.")
    if "allow_missing_files" in config and not isinstance(config["allow_missing_files"], bool):
        raise ConfigError("Configuration allow_missing_files must be a boolean.")
    for name in ('s3_prefix', 'batch_id'):
        if name in config:
            value = config[name]
            if not value.strip('/') or any(part in {'.', '..'} for part in value.split('/')) or '\\' in value or '\x00' in value:
                raise ConfigError(f'Configuration {name} contains an unsafe key segment.')
    for name in ('s3_private_bucket', 's3_public_bucket'):
        if name in config and not re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]', config[name]):
            raise ConfigError(f'Configuration {name} must be a DNS-style S3 bucket name.')
    additional = config.get('additional_files', [])
    if not isinstance(additional, list) or any(not isinstance(item, dict) or len(item) != 1 for item in additional):
        raise ConfigError('Configuration additional_files must be a list of single-field mappings.')
    keys = [key for item in additional for key in item]
    if any(not isinstance(key, str) or not key for key in keys) or len(keys) != len(set(keys)):
        raise ConfigError('Configuration additional_files needs unique, nonempty field names.')
    return config


def normalize_additional_files(config: Dict[str, object]) -> Dict[str, object]:
    raw_entries = config.get("additional_files", [])
    normalized: Dict[str, object] = {}
    if not isinstance(raw_entries, list):
        return normalized

    for entry in raw_entries:
        if isinstance(entry, dict):
            for key, value in entry.items():
                normalized[str(key)] = value
    return normalized
