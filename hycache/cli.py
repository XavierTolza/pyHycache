import click as click
from fuse import FUSE

from hycache.main import Hycache


@click.group()
def cli():
    pass


@cli.command()
@click.argument("mount_point")
@click.argument("ssd_path")
@click.argument("hdd_path")
def mount(mount_point, ssd_path, hdd_path):
    FUSE(Hycache(ssd_path, hdd_path), mount_point, nothreads=True, foreground=True)


if __name__ == '__main__':
    cli()