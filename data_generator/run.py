"""
CLI entry point for the data generator.

Usage:
    python -m data_generator.run                      # 5 GB default
    python -m data_generator.run --target-gb 1        # 1 GB test run
    python -m data_generator.run --target-gb 10 -t 8  # 10 GB, 8 threads
"""

import logging
import os

import click
import yaml

from .generator import TelemetryGenerator


def _load_config(path: str) -> dict:
    with open(path, "r") as f:
        return yaml.safe_load(f)


@click.command()
@click.option(
    "--config",
    "config_path",
    default="config/app_config.yaml",
    help="Path to YAML config file.",
)
@click.option(
    "--target-gb",
    type=float,
    default=None,
    help="Override target data volume in GB (default: value from config, typically 5).",
)
@click.option(
    "--batch-size",
    type=int,
    default=None,
    help="Records per Kafka produce batch.",
)
@click.option(
    "-t",
    "--threads",
    type=int,
    default=None,
    help="Number of parallel producer threads.",
)
@click.option(
    "--bootstrap-servers",
    default=None,
    help="Kafka bootstrap servers (overrides config).",
)
@click.option(
    "--topic",
    default=None,
    help="Kafka topic name (overrides config).",
)
def main(config_path, target_gb, batch_size, threads, bootstrap_servers, topic):
    """Generate synthetic vehicle telemetry and push to Kafka."""
    cfg = _load_config(config_path)

    # Configure logging
    log_cfg = cfg.get("logging", {})
    logging.basicConfig(
        level=getattr(logging, log_cfg.get("level", "INFO")),
        format=log_cfg.get("format", "%(asctime)s [%(levelname)s] %(name)s — %(message)s"),
    )

    kafka_cfg = cfg["kafka"]
    gen_cfg = cfg["generator"]

    # CLI overrides take precedence; inside Docker use the internal address
    if bootstrap_servers:
        servers = bootstrap_servers
    elif os.environ.get("DASHBOARD_DOCKER") == "1":
        servers = kafka_cfg.get("internal_bootstrap_servers", kafka_cfg["bootstrap_servers"])
    else:
        servers = kafka_cfg["bootstrap_servers"]
    topic_name = topic or kafka_cfg["topic"]
    target_bytes = int((target_gb or gen_cfg["target_bytes"] / 1e9) * 1e9)
    batch = batch_size or gen_cfg["batch_size"]
    n_threads = threads or gen_cfg["num_threads"]

    generator = TelemetryGenerator(
        bootstrap_servers=servers,
        topic=topic_name,
        state_topic=kafka_cfg["state_topic"],
        target_bytes=target_bytes,
        batch_size=batch,
        num_threads=n_threads,
        num_vehicles=gen_cfg["num_vehicles"],
    )

    summary = generator.run()

    click.echo("\n" + "=" * 60)
    click.echo("  DATA GENERATION SUMMARY")
    click.echo("=" * 60)
    click.echo(f"  Total Data    : {summary['total_bytes'] / 1e9:.2f} GB")
    click.echo(f"  Total Records : {summary['total_records']:,}")
    click.echo(f"  Elapsed Time  : {summary['elapsed_seconds']:.1f} seconds")
    click.echo(f"  Throughput    : {summary['throughput_mb_per_sec']:.1f} MB/s")
    click.echo(f"  Target        : {summary['target_bytes'] / 1e9:.2f} GB")
    click.echo("=" * 60)


if __name__ == "__main__":
    main()
