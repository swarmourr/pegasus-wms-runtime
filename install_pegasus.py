#!/usr/bin/env python3

import argparse
import getpass
import logging
import os
import platform
import shutil
import socket
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path
from urllib.request import urlretrieve


PEGASUS_REPO    = "https://github.com/swarmourr/pegasus-wms-runtime.git"
pegasus_version = "5.1.2"
htcondor_version = "25.x"

logging.basicConfig(level=logging.INFO, format="%(message)s")

def _install_wrappers(target_dir: Path, src_dir: Path):
    """
    Copy the pegasus-plan wrapper from the cloned repo into target_dir/wrappers/.

    This directory is placed BEFORE $PEGASUS_HOME/bin in PATH so every
    `pegasus-plan` call is intercepted: the wrapper injects the runtime
    prediction job (like stage-in/stage-out/cleanup) then delegates to
    the real Java planner via $PEGASUS_HOME/bin/pegasus-plan.
    """
    wrappers_dir = target_dir / "wrappers"
    wrappers_dir.mkdir(parents=True, exist_ok=True)

    repo_wrapper = src_dir / "wrappers" / "pegasus-plan"
    if not repo_wrapper.exists():
        raise FileNotFoundError(
            f"pegasus-plan wrapper not found in cloned repo: {repo_wrapper}"
        )

    wrapper = wrappers_dir / "pegasus-plan"
    shutil.copy2(str(repo_wrapper), str(wrapper))
    wrapper.chmod(0o755)
    logging.info(f"Installed pegasus-plan wrapper → {wrapper}")


def install_pegasus(target_dir: Path, arch: str, os_name: str, os_version: str):
    pegasus_dir = target_dir / "pegasus"

    # ── Step 1: download binary Pegasus (provides pegasus-plan, pegasus-config, etc.) ──
    base_url     = f"https://download.pegasus.isi.edu/pegasus/{pegasus_version}"
    if os_name == "debian":
        p_os_name    = "deb"
        debian_map   = {"10": "10", "11": "11", "12": "12", "24": "12"}
        p_os_version = debian_map.get(os_version, "12")
    else:
        p_os_name    = os_name
        p_os_version = os_version

    tarball_name = f"pegasus-binary-{pegasus_version}-{arch}_{p_os_name}_{p_os_version}.tar.gz"
    tarball_path = target_dir / tarball_name

    logging.info(f"Downloading Pegasus binary: {tarball_name}")
    urlretrieve(f"{base_url}/{tarball_name}", tarball_path)

    with tarfile.open(tarball_path, "r:gz") as tar:
        if sys.version_info >= (3, 9):
            tar.extractall(path=target_dir, filter="tar")
        else:
            tar.extractall(path=target_dir)
    tarball_path.unlink()
    next(target_dir.glob("pegasus-*")).rename(pegasus_dir)

    # ── Step 2: clone runtime-prediction Python packages on top ──────────────
    src_dir = target_dir / "pegasus-wms-runtime-src"
    logging.info(f"Cloning Python packages from {PEGASUS_REPO}")
    subprocess.run(
        ["git", "clone", "--depth=1", PEGASUS_REPO, str(src_dir)],
        check=True,
    )

    packages_dir = src_dir / "packages"
    for pkg in ["pegasus-common", "pegasus-worker", "pegasus-api", "pegasus-python"]:
        pkg_path = packages_dir / pkg
        if pkg_path.exists():
            logging.info(f"Installing Python package: {pkg}")
            subprocess.run(
                [sys.executable, "-m", "pip", "install", "-e", str(pkg_path), "--quiet"],
                check=True,
            )

    # ── Step 3: install ML dependencies ──────────────────────────────────────
    logging.info("Installing runtime-prediction dependencies")
    subprocess.run(
        [sys.executable, "-m", "pip", "install",
         "torch", "scikit-learn", "numpy", "pandas", "--quiet"],
        check=True,
    )

    # ── Step 4: install pegasus-plan wrapper ──────────────────────────────────
    # wrappers/ sits BEFORE $PEGASUS_HOME/bin in PATH so our Python wrapper
    # intercepts `pegasus-plan` and injects the runtime prediction job natively
    # (like stage-in/stage-out/cleanup) before delegating to the real Java planner.
    _install_wrappers(target_dir, src_dir)



def install_htcondor(target_dir: Path, arch: str, os_name: str, os_version: str):
    base_url = f"https://htcss-downloads.chtc.wisc.edu/tarball/{htcondor_version}/current"
    
    c_os_name = ""
    c_os_version = os_version

    if os_name == "debian":
        c_os_name = "Debian"
        debian_version_map = {
            "10": "10",
            "11": "11",
            "12": "12",
            "24": "12",
        }
        c_os_version = debian_version_map.get(os_version, "12")
    elif os_name == "rhel":
        c_os_name = "AlmaLinux"
    elif os_name == "suse":
        c_os_name = "AlmaLinux"
    elif os_name == "macos":
        c_os_name = "macOS"
        c_os_version = ""
    else:
        raise ValueError(f"Unable to determine HTCondor tarball for {os_name}")

    tarball_name = f"condor-{arch}_{c_os_name}{c_os_version}-stripped.tar.gz"
    tarball_path = target_dir / tarball_name
    
    logging.info(f"Downloading HTCondor tarball: {tarball_name}")
    urlretrieve(f"{base_url}/{tarball_name}", tarball_path)

    with tarfile.open(tarball_path, "r:gz") as tar:
        if sys.version_info >= (3, 9):
            tar.extractall(path=target_dir, filter="tar")
        else:
            tar.extractall(path=target_dir)

    tarball_path.unlink()
    
    condor_dir = next(target_dir.glob("condor-*"))
    condor_dir.rename(target_dir / "condor")


def _env_exports(target_dir: Path) -> str:
    """Return shell export statements for the Pegasus + Condor environment."""
    pegasus_bin  = target_dir / "pegasus" / "bin"
    wrappers_bin = target_dir / "wrappers"
    return (
        f"export PEGASUS_HOME={target_dir}/pegasus\n"
        f"export CONDOR_CONFIG={target_dir}/condor/condor.conf\n"
        # wrappers_bin MUST come before pegasus_bin so our pegasus-plan
        # wrapper intercepts every call before the real Java planner
        f"export PATH={wrappers_bin}:{pegasus_bin}:{target_dir}/condor/bin:{target_dir}/condor/sbin:$PATH\n"
    )


def env_setup(target_dir: Path):
    # Write env.sh so the user can source it
    env_sh_path = target_dir / "env.sh"
    with env_sh_path.open("w") as f:
        f.write(_env_exports(target_dir))

    # Also apply to the current process (used by configure())
    pegasus_bin = target_dir / "pegasus" / "bin"
    path = os.environ.get("PATH", "")
    os.environ["PATH"] = ":".join([
        str(pegasus_bin),
        str(target_dir / "condor/bin"),
        str(target_dir / "condor/sbin"),
        path,
    ])
    os.environ["CONDOR_CONFIG"] = str(target_dir / "condor/condor.conf")
    os.environ["PEGASUS_HOME"]  = str(target_dir / "pegasus")


def configure(target_dir: Path):
    condor_config_path = target_dir / "condor" / "condor.conf"
    with condor_config_path.open("w") as f:
        f.write(
            f"""
RELEASE_DIR = {target_dir}/condor
LOCAL_DIR = $(RELEASE_DIR)/local
REQUIRE_LOCAL_CONFIG_FILE = false
RUN     = $(LOCAL_DIR)/run
LOG     = $(LOCAL_DIR)/log
LOCK    = $(LOCAL_DIR)/lock
SPOOL   = $(LOCAL_DIR)/spool
EXECUTE = $(LOCAL_DIR)/execute
BIN     = $(RELEASE_DIR)/bin
LIB     = $(RELEASE_DIR)/lib64/condor
INCLUDE = $(RELEASE_DIR)/include/condor
SBIN    = $(RELEASE_DIR)/sbin
LIBEXEC = $(RELEASE_DIR)/libexec
SHARE   = $(RELEASE_DIR)/usr/share/condor

PROCD_ADDRESS = $(RUN)/procd_pipe

DAEMON_LIST = MASTER, COLLECTOR, SCHEDD, NEGOTIATOR, STARTD

CONDOR_HOST = localhost
NETWORK_INTERFACE = 127.0.0.1

USE_SHARED_PORT = False
COLLECTOR_PORT = {10000 + int.from_bytes(os.urandom(2), 'big') % 40000}
COLLECTOR_USES_SHARED_PORT = False

# idtokens - good base for pilots
SEC_PASSWORD_DIRECTORY = $(RELEASE_DIR)/etc/passwords.d
SEC_TOKEN_SYSTEM_DIRECTORY = $(RELEASE_DIR)/etc/tokens.d
SEC_TOKEN_DIRECTORY = $(SEC_TOKEN_SYSTEM_DIRECTORY)

SEC_DEFAULT_AUTHENTICATION = REQUIRED
SEC_DEFAULT_ENCRYPTION = REQUIRED
SEC_DEFAULT_INTEGRITY = REQUIRED
SEC_DEFAULT_AUTHENTICATION_METHODS = FS, IDTOKEN
SEC_CLIENT_AUTHENTICATION_METHODS = $(SEC_DEFAULT_AUTHENTICATION_METHODS)
# With strong security, do not use IP based controls
ALLOW_WRITE = *
ALLOW_READ = *
ALLOW_ADMINISTRATOR = {getpass.getuser()}@{socket.getfqdn()}

# dynamic slots
SLOT_TYPE_1 = cpus=100%,disk=100%,swap=100%
SLOT_TYPE_1_PARTITIONABLE = TRUE
NUM_SLOTS = 1
NUM_SLOTS_TYPE_1 = 1
"""
        )

    dirs_to_create = [
        "run",
        "log",
        "lock",
        "spool",
        "execute"
    ]
    for dir_var in dirs_to_create:
        dir_path = target_dir / "condor" / "local" / dir_var
        if not Path(dir_path).exists():
                Path(dir_path).mkdir(parents=True)

    dirs_to_create = [
        "tokens.d",
        "passwords.d",
    ]
    for dir_var in dirs_to_create:
        dir_path = target_dir / "condor" / "etc" / dir_var
        if not Path(dir_path).exists():
                Path(dir_path).mkdir(parents=True)

    pool_password_path = target_dir / "condor/etc/passwords.d/POOL"
    with open(pool_password_path, "wb") as f:
        f.write(os.urandom(128))
    pool_password_path.chmod(0o600)

    personal_token_path = target_dir / "condor/etc/tokens.d/personal.token"
    user_at_host = f"{getpass.getuser()}@{socket.getfqdn()}"
    
    # Need to make sure condor bins are on the path and CONDOR_CONFIG is set
    os.environ["PATH"] = str(target_dir / "condor/bin") + ":" + os.environ["PATH"]
    os.environ["CONDOR_CONFIG"] = str(condor_config_path)
    
    # Create token
    with personal_token_path.open("w") as f:
        subprocess.run(
            [
                "condor_token_create",
                "-key",
                "POOL",
                "-identity",
                user_at_host,
            ],
            stdout=f,
            check=True,
        )
    personal_token_path.chmod(0o600)

    # pegasus-configure-glite is only available in binary distributions — skip


def success_message(target_dir: Path):
    logging.info(
        f"""
Pegasus WMS installed successfully into {target_dir}

Apply environment variables to your shell:

    source {target_dir}/env.sh

Or use eval to apply inline without a file:

    eval "$(python3 install_pegasus.py --print-env --target {target_dir})"

Then start HTCondor with:

    condor_master

You should then be able to run:

    condor_status
    condor_q
    pegasus-version

You are ready to submit workflows!
"""
    )


def get_system():
    arch = platform.machine()
    os_name = platform.system()
    os_version = platform.release()

    if os_name == "Linux":
        try:
            with open("/etc/os-release", "r") as f:
                os_release_info = {}
                for line in f:
                    line = line.strip()
                    if "=" in line:
                        key, value = line.split("=", 1)
                        os_release_info[key] = value.strip('"')
            os_name = os_release_info["ID"]
            os_version = os_release_info["VERSION_ID"]
        except FileNotFoundError:
            # if /etc/os-release is not available, we can't determine the distro
            raise OSError("Unable to determine Linux distribution")

        os_map = {
            "debian": "debian",
            "ubuntu": "debian",
            "centos": "rhel",
            "rocky": "rhel",
            "scientific": "rhel",
            "almalinux": "rhel",
            "fedora": "fedora",
            "sles": "suse",
            "opensuse-leap": "suse",
            "opensuse-tumbleweed": "suse",
        }
        os_name = os_map.get(os_name, os_name)

        os_version = os_version.split(".")[0]

    elif os_name == "Darwin":
        os_name = "macos"
        os_version = platform.mac_ver()[0].split(".")[0]

    else:
        raise OSError("Unsupported operating system")

    if not all([os_name, os_version, arch]) or "UNKNOWN" in [
        os_name,
        os_version,
        arch,
    ]:
        raise OSError("Failed to get system info")

    return arch, os_name, os_version


def main():
    parser = argparse.ArgumentParser(description="Install Pegasus WMS.")
    parser.add_argument(
        "--target",
        type=Path,
        default=None,
        help="Target directory for installation",
    )
    parser.add_argument(
        "--pegasus-version",
        default=pegasus_version,
        help=f"Pegasus version to install (default: {pegasus_version})",
    )
    parser.add_argument(
        "--print-env",
        action="store_true",
        default=False,
        help="Print shell export statements for the environment and exit. "
             "Use with eval: eval \"$(python3 install_pegasus.py --print-env --target DIR)\"",
    )
    args = parser.parse_args()

    if args.target is None:
        args.target = Path.cwd() / f"pegasus-{args.pegasus_version}"

    target_dir = args.target.resolve()

    # --print-env: just emit the export lines and exit (no install)
    if args.print_env:
        print(_env_exports(target_dir), end="")
        sys.exit(0)

    if target_dir.exists():
        if not target_dir.is_dir():
            logging.error(f"ERROR: target path ({target_dir}) exists and is not a directory. Unable to continue")
            exit(1)
        logging.info(f"Target directory ({target_dir}) already exists; deleting it")
        shutil.rmtree(target_dir)

    logging.info(f"Will install into {target_dir}")
    target_dir.mkdir(parents=True)

    try:
        arch, os_name, os_version = get_system()
        logging.info(f"Arch: {arch}    Base OS: {os_name}    OS Version: {os_version}")

        install_pegasus(target_dir, arch, os_name, os_version)
        install_htcondor(target_dir, arch, os_name, os_version)
        env_setup(target_dir)
        configure(target_dir)
        success_message(target_dir)
    except Exception as e:
        logging.error(f"An error occurred: {e}")
        exit(1)

if __name__ == "__main__":
    main()
