import json
import sys
import tarfile
from pathlib import Path
from typing import Annotated, TypedDict

import typer

app = typer.Typer()


def ceil_div(a, b):
    """Calculate ceiling division"""
    return (a + b - 1) // b


class FileInfo(TypedDict):
    path: str
    layer: str
    offset: int
    size: int


def log(*o):
    print("LOG:", *o, file=sys.stderr)


def _index_oci_archive(path: Path):
    # Read file from path
    with tarfile.open(path, mode="r:*") as tar:
        files: list[FileInfo] = []
        log("Starting to read tar stream")
        current_offset = 0

        # Check that is an oci archive
        try:
            if not all(tar.getmember(f).isreg() for f in ["oci-layout", "index.json"]):
                raise RuntimeError("Invalid oci-archive structture")
        except KeyError:
            RuntimeError("Invalid oci-archive structture")

        # Iterate through top-level tar members
        for member in tar.getmembers():
            if member.name.startswith("blobs/sha256/") and member.isreg():
                # Identified layer tarball
                layer_name = member.name
                log(f"Found layer file {layer_name}")
                data_offset = (
                    current_offset + 512
                )  # Data starts after the 512-byte header
                size = member.size

                # Extract the layer tarball data
                _layer_data = tar.extractfile(member)
                if not _layer_data:
                    raise RuntimeError("Couldnt extract file from tarball", member)
                with _layer_data as layer_data:
                    try:
                        with tarfile.open(fileobj=layer_data, mode="r|*") as layer_tar:
                            layer_current_offset = 0
                            log(f"Reading layer {member.name}")

                            # Iterate through files in the layer tarball
                            for layer_member in layer_tar.getmembers():
                                if layer_member.isreg():
                                    # Calculate offsets for regular files
                                    layer_data_offset = layer_current_offset + 512
                                    absolute_offset = data_offset + layer_data_offset

                                    # Collect file information
                                    file_info = FileInfo(
                                        path="/" + layer_member.name.lstrip("/"),
                                        layer=layer_name,
                                        offset=absolute_offset,
                                        size=layer_member.size,
                                    )
                                    files.append(file_info)

                                    # Update offset within layer tarball
                                    data_blocks = ceil_div(size, 512) * 512
                                    layer_current_offset += 512 + data_blocks * 512
                    except tarfile.ReadError:
                        continue
                    layer_data.close()  # Close layer's tar stream

                # Update offset in top-level tar
                data_total_size = ceil_div(size, 512) * 512
                current_offset += 512 + data_total_size

        # Output JSON to stdout
        json.dump({"files": files}, sys.stdout, indent=2)


@app.command()
def main(
    oci_archive_path: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=True,
            readable=True,
        ),
    ],
):
    _index_oci_archive(oci_archive_path)


if __name__ == "__main__":
    app()
