import subprocess
import sys
import os
import shutil
import stat
from cogs import bot_startup_display as startup

# Python version check
MIN_PYTHON = (3, 11)

if sys.version_info < MIN_PYTHON:
    startup.python_too_old(f"{MIN_PYTHON[0]}.{MIN_PYTHON[1]}", f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")
    sys.exit(1)

def is_container() -> bool:
    # Docker, Kubernetes, Podman - simple marker file checks
    marker_files = ["/.dockerenv", "/var/run/secrets/kubernetes.io", "/run/.containerenv"]
    if any(os.path.exists(path) for path in marker_files):
        return True

    # LXC - check init process environment
    try:
        with open("/proc/1/environ", "r") as f:
            if "container=lxc" in f.read():
                return True
    except (IOError, OSError):
        pass

    # Systemd-nspawn - check container type file
    try:
        with open("/run/systemd/container", "r") as f:
            if f.read() == "systemd-nspawn\n":
                return True
    except (IOError, OSError):
        pass

    return False

def is_ci_environment() -> bool:
    """Check if running in a CI environment"""
    ci_indicators = [
        'CI', 'CONTINUOUS_INTEGRATION', 'GITHUB_ACTIONS', 
        'JENKINS_URL', 'TRAVIS', 'CIRCLECI', 'GITLAB_CI'
    ]
    return any(os.getenv(indicator) for indicator in ci_indicators)

def break_system_packages() -> bool:
    """Check if the user is certain about breaking system packages"""
    return "--break-system-packages" in sys.argv

def break_system_packages_arg() -> bool:
    return break_system_packages() and not should_skip_venv()

def should_skip_venv() -> bool:
    """Check if venv should be skipped"""
    
    if ("--no-venv" in sys.argv) and (sys.platform.startswith("linux")) and (not is_container()) and (not is_ci_environment()) and (not break_system_packages()):
        startup.error_box(
            "Virtual Environment Required",
            "On Linux, running without a virtual environment won't work unless you break system packages.",
            "Add --break-system-packages to confirm you understand the risks."
        )
        sys.exit(1)
    
    return '--no-venv' in sys.argv or is_container() or is_ci_environment()

# Handle venv setup
if sys.prefix == sys.base_prefix and not should_skip_venv():
    venv_path = "bot_venv"

    # Determine the python executable path in the venv
    if sys.platform == "win32":
        venv_python_name = os.path.join(venv_path, "Scripts", "python.exe")
        activate_script = os.path.join(venv_path, "Scripts", "activate.bat")
    else:
        venv_python_name = os.path.join(venv_path, "bin", "python")
        activate_script = os.path.join(venv_path, "bin", "activate")

    if not os.path.exists(venv_path):
        try:
            startup.phase_start("Setting up virtual environment")
            subprocess.check_call([sys.executable, "-m", "venv", venv_path], timeout=300)

            if sys.platform == "win32":
                startup.venv_instructions(venv_python_name, sys.platform)
                sys.exit(0)
            else: # For non-Windows, try to relaunch automatically
                startup.venv_instructions(venv_python_name, sys.platform)
                venv_python_executable = os.path.join(venv_path, "bin", "python")
                os.execv(venv_python_executable, [venv_python_executable] + sys.argv)

        except Exception as e:
            startup.error_box("Virtual Environment Failed", str(e), "python -m venv bot_venv")
            sys.exit(1)
    else: # Venv exists
        if sys.platform == "win32":
            startup.venv_exists_instructions(venv_python_name, sys.platform)
            sys.exit(0)
        elif '--no-venv' in sys.argv:
            pass  # Silent, not important
        else: # For non-Windows, if venv exists but we're not in it, try to relaunch
            venv_python_executable = os.path.join(venv_path, "bin", "python")
            if os.path.exists(venv_python_executable):
                os.execv(venv_python_executable, [venv_python_executable] + sys.argv)
            else:
                startup.error_box("Virtual Environment Corrupted", "Please delete bot_venv and run again.")
                sys.exit(1)

try: # Import or install requests so we can get the requirements
    import requests
except ImportError:
    try:
        cmd = [sys.executable, "-m", "pip", "install", "requests"]

        if break_system_packages_arg():
            cmd.append("--break-system-packages")

        subprocess.check_call(cmd, timeout=300, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        import requests
    except Exception as e:
        startup.error_box("Dependency Error", f"Failed to install requests: {e}", "pip install requests")
        sys.exit(1)

def remove_readonly(func, path, _):
    """Clear the readonly bit and reattempt the removal"""
    os.chmod(path, stat.S_IWRITE)
    func(path)

def safe_remove(path, is_dir=None):
    """
    Safely remove a file or directory.
    Clear the read-only bit on Windows.
    
    Args:
        path: Path to file or directory to remove
        is_dir: True for directory, False for file, None to auto-detect
        
    Returns:
        bool: True if successfully removed, False otherwise
    """
    if not os.path.exists(path):
        return True  # Already gone, consider it success
    
    if is_dir is None: # Auto-detect type if not specified
        is_dir = os.path.isdir(path)
    
    try:
        if is_dir:
            if sys.platform == "win32":
                if sys.version_info >= (3, 12): # check if python version >= 3.12 for onexc support
                    shutil.rmtree(path, onexc=remove_readonly)
                else:
                    shutil.rmtree(path, onerror=remove_readonly)
            else:
                shutil.rmtree(path)
        else:
            try:
                os.remove(path)
            except PermissionError:
                if sys.platform == "win32":
                    os.chmod(path, stat.S_IWRITE)
                    os.remove(path)
                else:
                    raise  # Re-raise on non-Windows platforms
        
        return True
        
    except PermissionError:
        print(f"Warning: Access Denied. Could not remove '{path}'.\nCheck permissions or if {'directory' if is_dir else 'file'} is in use.")
    except OSError as e:
        print(f"Warning: Could not remove '{path}': {e}")
    
    return False

def calculate_file_hash(filepath):
    """Calculate SHA256 hash of a file."""
    import hashlib
    if not os.path.exists(filepath):
        return None
    
    sha256_hash = hashlib.sha256()
    try:
        with open(filepath, "rb") as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()
    except Exception:
        return None

def uninstall_packages(packages, reason=""):
    """Generic function to uninstall a list of packages"""
    if not packages:
        return
    
    print(f"Found {len(packages)} packages to remove{reason}: {', '.join(packages)}")
    debug_mode = "--verbose" in sys.argv or "--debug" in sys.argv
    
    for package in packages:
        try:
            cmd = [sys.executable, "-m", "pip", "uninstall", "-y", package]
            
            if debug_mode:
                subprocess.check_call(cmd, timeout=300)
            else:
                subprocess.check_call(cmd, timeout=300, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            pass  # Silent removal
        except subprocess.CalledProcessError:
            pass  # Silent, package might be needed by others
        except Exception:
            pass  # Silent removal failure

def get_packages_to_remove():
    """Get all packages that should be removed (from requirements comparison + legacy)"""
    packages_to_remove = set()
    
    # Check requirements.old vs requirements.txt (if they exist)
    if os.path.exists("requirements.old") and os.path.exists("requirements.txt"):
        try:
            old_packages = set()
            new_packages = set()
            
            # Parse old requirements
            with open("requirements.old", "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        pkg_name = line.split("==")[0].split(">=")[0].split("<=")[0].split("~=")[0].split("!=")[0]
                        old_packages.add(pkg_name.strip().lower())
            
            # Parse new requirements
            with open("requirements.txt", "r") as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith("#"):
                        pkg_name = line.split("==")[0].split(">=")[0].split("<=")[0].split("~=")[0].split("!=")[0]
                        new_packages.add(pkg_name.strip().lower())
            
            packages_to_remove.update(old_packages - new_packages)
        except Exception as e:
            pass  # Silent error comparing requirements
    
    # Always check for legacy packages that are still installed
    for package in LEGACY_PACKAGES_TO_REMOVE:
        if is_package_installed(package):
            packages_to_remove.add(package.lower())
    
    return list(packages_to_remove)

def cleanup_removed_packages():
    """Main cleanup function - removes obsolete packages"""
    packages = get_packages_to_remove()
    
    if packages:
        reason = " from requirements" if os.path.exists("requirements.old") else " (legacy packages)"
        uninstall_packages(packages, reason)
    
    # Clean up requirements.old
    if os.path.exists("requirements.old"):
        safe_remove("requirements.old", is_dir=False)

# Potential leftovers from older bot versions
LEGACY_PACKAGES_TO_REMOVE = [
    "ddddocr",
    "easyocr",
    "torch",
    "torchvision",
    "torchaudio",
]

def has_obsolete_requirements():
    """
    Check if requirements.txt contains obsolete packages from older versions.
    Required to fix bug with v1.2.0 upgrade logic that deleted new requirements.txt.
    """
    if not os.path.exists("requirements.txt"):
        return False
    
    try:
        with open("requirements.txt", "r") as f:
            content = f.read().lower()
            
        for package in LEGACY_PACKAGES_TO_REMOVE:
            if package.lower() in content:
                return True
        
        return False
    except Exception:
        return False

def is_package_installed(package_name):
    """Check if a package is installed"""
    try:
        result = subprocess.run(
            [sys.executable, "-m", "pip", "show", package_name],
            capture_output=True,
            text=True,
            timeout=30
        )
        return result.returncode == 0
    except Exception:
        return False


# Configuration for multiple update sources
UPDATE_SOURCES = [
    {
        "name": "GitHub",
        "api_url": "https://api.github.com/repos/whiteout-project/bot/releases/latest",
        "primary": True
    },
    {
        "name": "GitLab",
        "api_url": "https://gitlab.whiteout-bot.com/api/v4/projects/1/releases",
        "project_id": 1,
        "primary": False
    }
    # Can add more sources here as needed
]

def get_latest_release_info(beta_mode=False):
    """Try to get latest release info from multiple sources."""
    for source in UPDATE_SOURCES:
        try:
            startup.phase_start(f"Checking for updates ({source['name']})")
            
            if source['name'] == "GitHub":
                if beta_mode:
                    # Get latest commit from main branch
                    repo_name = source['api_url'].split('/repos/')[1].split('/releases')[0]
                    branch_url = f"https://api.github.com/repos/{repo_name}/branches/main"
                    response = requests.get(branch_url, timeout=30)
                    if response.status_code == 200:
                        data = response.json()
                        commit_sha = data['commit']['sha'][:7]  # Short SHA
                        return {
                            "tag_name": f"beta-{commit_sha}",
                            "body": f"Latest development version from main branch (commit: {commit_sha})",
                            "download_url": f"https://github.com/{repo_name}/archive/refs/heads/main.zip",
                            "source": f"{source['name']} (Beta)"
                        }
                else:
                    response = requests.get(source['api_url'], timeout=30)
                    if response.status_code == 200:
                        data = response.json()
                        # Use GitHub's automatic source archive
                        repo_name = source['api_url'].split('/repos/')[1].split('/releases')[0]
                        download_url = f"https://github.com/{repo_name}/archive/refs/tags/{data['tag_name']}.zip"
                        return {
                            "tag_name": data["tag_name"],
                            "body": data["body"],
                            "download_url": download_url,
                            "source": source['name']
                        }
                    
            elif source['name'] == "GitLab":
                response = requests.get(source['api_url'], timeout=30)
                if response.status_code == 200:
                    releases = response.json()
                    if releases:
                        latest = releases[0]  # GitLab returns array, first is latest
                        tag_name = latest['tag_name']
                        # Use GitLab's source archive
                        download_url = f"https://gitlab.whiteout-bot.com/whiteout-project/bot/-/archive/{tag_name}/bot-{tag_name}.zip"
                        return {
                            "tag_name": tag_name,
                            "body": latest.get("description", "No release notes available"),
                            "download_url": download_url,
                            "source": source['name']
                        }
            
            # Add handling for other sources here
            
        except requests.exceptions.RequestException as e:
            continue
        except Exception:
            continue

    startup.phase_fail("Update check failed (all sources unreachable)")
    return None

def download_requirements_from_release(beta_mode=False):
    """
    Download requirements.txt file directly from the latest release or main branch if beta mode.
    """
    if os.path.exists("requirements.txt"):
        return True
    
    # Get latest release info to find the tag
    release_info = get_latest_release_info(beta_mode=beta_mode)
    if not release_info:
        return False
    
    tag = release_info["tag_name"]
    source_name = release_info.get("source", "Unknown")
    
    # Build raw URL based on source and mode
    if source_name == "GitHub" or "GitHub" in source_name:
        if beta_mode:
            raw_url = "https://raw.githubusercontent.com/whiteout-project/bot/main/requirements.txt"
        else:
            raw_url = f"https://raw.githubusercontent.com/whiteout-project/bot/refs/tags/{tag}/requirements.txt"
    elif source_name == "GitLab":
        if beta_mode:
            raw_url = "https://gitlab.whiteout-bot.com/whiteout-project/bot/-/raw/main/requirements.txt"
        else:
            raw_url = f"https://gitlab.whiteout-bot.com/whiteout-project/bot/-/raw/{tag}/requirements.txt"
    else:
        return False

    try:
        response = requests.get(raw_url, timeout=30)

        if response.status_code == 200:
            with open("requirements.txt", "w") as f:
                f.write(response.text)
            return True
        else:
            return False

    except Exception:
        return False

def _import_onnxruntime_quietly():
    """Import onnxruntime while suppressing C++ GPU discovery warning."""
    # Redirect fd 2 (C-level stderr) since ONNX writes there, not to sys.stderr
    _fd, _null = sys.stderr.fileno(), os.open(os.devnull, os.O_WRONLY)
    _bak = os.dup(_fd); os.dup2(_null, _fd); os.close(_null)
    try:
        import onnxruntime
        return onnxruntime
    finally:
        os.dup2(_bak, _fd); os.close(_bak)

def is_onnxruntime_nightly():
    """Check if installed onnxruntime is a nightly build."""
    try:
        onnxruntime = _import_onnxruntime_quietly()
        version = onnxruntime.__version__
        # Nightly versions contain 'dev' or '+' (e.g., "1.20.0.dev20251115001")
        return "dev" in version or "+" in version
    except ImportError:
        return False

def install_onnxruntime_nightly():
    """Install onnxruntime from nightly feed for Python 3.14+ compatibility."""
    cmd = [
        sys.executable, "-m", "pip", "install", "--pre",
        "--extra-index-url", "https://aiinfra.pkgs.visualstudio.com/PublicPackages/_packaging/ORT-Nightly/pypi/simple/",
        "onnxruntime", "--no-cache-dir"
    ]
    if break_system_packages_arg():
        cmd.append("--break-system-packages")
    result = subprocess.run(cmd, timeout=1200, capture_output=True, text=True)
    if result.returncode != 0:
        raise subprocess.CalledProcessError(result.returncode, cmd)

def install_onnxruntime_stable(version_spec="onnxruntime>=1.18.1"):
    """Install onnxruntime from stable PyPI."""
    cmd = [sys.executable, "-m", "pip", "install", version_spec, "--no-cache-dir", "--force-reinstall"]
    if break_system_packages_arg():
        cmd.append("--break-system-packages")
    subprocess.check_call(cmd, timeout=1200, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def check_and_install_requirements():
    """Check each requirement and install missing ones."""
    if not os.path.exists("requirements.txt"):
        return False

    # Read requirements
    with open("requirements.txt", "r") as f:
        requirements = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    
    missing_packages = []
    
    # Test each requirement
    for requirement in requirements:
        package_name = requirement.split("==")[0].split(">=")[0].split("<=")[0].split("~=")[0].split("!=")[0]
        
        try:
            if package_name == "discord.py":
                import discord
            elif package_name == "aiohttp-socks":
                import aiohttp_socks
            elif package_name == "python-dotenv":
                import dotenv
            elif package_name == "python-bidi":
                import bidi
            elif package_name == "arabic-reshaper":
                import arabic_reshaper
            elif package_name.lower() == "pillow":
                import PIL
            elif package_name.lower() == "numpy":
                import numpy
            elif package_name.lower() == "onnxruntime":
                _import_onnxruntime_quietly()
                # Check if we need to switch versions based on Python version
                if sys.version_info >= (3, 14) and not is_onnxruntime_nightly():
                    # Has stable but needs nightly - mark for reinstall
                    raise ImportError("Need nightly for Python 3.14+")
                elif sys.version_info < (3, 14) and is_onnxruntime_nightly():
                    # Has nightly but can use stable - mark for reinstall
                    raise ImportError("Should use stable for Python <3.14")
            else:
                __import__(package_name)
                        
        except ImportError:
            missing_packages.append(requirement)

    if missing_packages: # Install missing packages
        startup.phase_start("Installing missing packages")

        for package in missing_packages:
            package_name = package.split("==")[0].split(">=")[0].split("<=")[0].split("~=")[0].split("!=")[0]

            # Handle onnxruntime specially based on Python version
            if package_name.lower() == "onnxruntime":
                if sys.version_info >= (3, 14):
                    install_onnxruntime_nightly()
                else:
                    install_onnxruntime_stable(package)
                continue

            try:
                cmd = [sys.executable, "-m", "pip", "install", package, "--no-cache-dir"]

                if break_system_packages_arg():
                    cmd.append("--break-system-packages")

                subprocess.check_call(cmd, timeout=1200, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

            except Exception as e:
                startup.phase_fail("Dependencies failed", details=[f"Failed to install {package}: {e}"], fix="pip install -r requirements.txt")
                return False

    startup.phase_ok("Dependencies satisfied")
    return True

def setup_dependencies(beta_mode=False):
    """Main function to set up all dependencies."""
    startup.phase_start("Checking dependencies")

    removed_obsolete = False
    if has_obsolete_requirements():
        removed_obsolete = True
        safe_remove("requirements.txt", is_dir=False)

    if not os.path.exists("requirements.txt"):
        if not download_requirements_from_release(beta_mode=beta_mode):
            startup.phase_fail("Dependencies failed", details=["Could not download requirements.txt"], fix="Download the complete bot package from: https://github.com/whiteout-project/bot/releases")
            return False

    if not check_and_install_requirements():
        startup.phase_fail("Dependencies failed", fix="pip install -r requirements.txt")
        return False
    
    return True

beta_mode = "--beta" in sys.argv
if not setup_dependencies(beta_mode=beta_mode):
    pass  # Warnings already shown by setup_dependencies

try:
    from colorama import Fore, Style, init
    import discord
except ImportError as e:
    startup.error_box("Import Failed", f"Import failed after dependency setup: {e}", "Restart the script or run: pip install -r requirements.txt")
    sys.exit(1)

# Colorama shortcuts
F = Fore
R = Style.RESET_ALL

import warnings
import aiohttp

def check_vcredist():
    """Check if Visual C++ Redistributable is installed on Windows."""
    if sys.platform != "win32":
        return True  # Not applicable on non-Windows

    try:
        import winreg
        import struct

        # Determine Python architecture
        is_64bit = struct.calcsize("P") * 8 == 64
        arch = "x64" if is_64bit else "x86"

        # Registry key for VC++ 2015-2022 runtime
        key_path = f"SOFTWARE\\Microsoft\\VisualStudio\\14.0\\VC\\Runtimes\\{arch}"

        try:
            key = winreg.OpenKey(winreg.HKEY_LOCAL_MACHINE, key_path)
            winreg.CloseKey(key)
            return True  # VC++ Redist is installed
        except FileNotFoundError:
            # VC++ Redist not found - show warning
            download_url = f"https://aka.ms/vc14/vc_redist.{arch}.exe"
            startup.phase_fail(f"Visual C++ Redistributable ({arch}) not found",
                details=["Gift code redemption (captcha solver) will not work until this is installed."],
                fix=f"Download from: {download_url}")
            return False

    except Exception:
        return True  # If we can't check, we hope it's fine

def startup_cleanup():
    """Perform all cleanup tasks on startup - directories, files, and legacy packages."""
    v1_path = "V1oldbot"
    if os.path.exists(v1_path):
        safe_remove(v1_path)

    v2_path = "V2Old"
    if os.path.exists(v2_path):
        safe_remove(v2_path)

    pictures_path = "pictures"
    if os.path.exists(pictures_path):
        safe_remove(pictures_path)

    txt_path = "autoupdateinfo.txt"
    if os.path.exists(txt_path):
        safe_remove(txt_path)
    
    # Check for legacy packages to remove on startup
    legacy_packages = []
    for package in LEGACY_PACKAGES_TO_REMOVE:
        if is_package_installed(package):
            legacy_packages.append(package.lower())
    
    if legacy_packages:
        uninstall_packages(legacy_packages, " (legacy packages)")

startup_cleanup()
check_vcredist()

warnings.filterwarnings("ignore", category=DeprecationWarning)

init(autoreset=True)

try:
    import ssl
    import certifi

    def _create_ssl_context_with_certifi():
        return ssl.create_default_context(cafile=certifi.where())
    
    original_create_default_https_context = getattr(ssl, "_create_default_https_context", None)

    if original_create_default_https_context is None or \
       original_create_default_https_context is ssl.create_default_context:
        ssl._create_default_https_context = _create_ssl_context_with_certifi
        
        pass  # SSL context patch applied silently
    else: # Assume if it's already patched, it's for a good reason
        pass  # SSL context already modified, skip
except ImportError:
    pass  # Certifi not found, SSL verification might fail
except Exception:
    pass  # SSL patch error, continue anyway

if __name__ == "__main__":
    # ── Proxy support (--proxy flag) ───────────────────────────────────────────
    # Pass --proxy <url> to route all game API traffic through a proxy server.
    # Example: python main.py --proxy http://192.168.1.10:18080
    # If --proxy is passed without a URL, defaults to http://localhost:18080.
    # This is required when running behind proxysolution-network.
    _proxy_url = None
    if "--proxy" in sys.argv:
        _proxy_idx = sys.argv.index("--proxy")
        _proxy_url = (
            sys.argv[_proxy_idx + 1]
            if _proxy_idx + 1 < len(sys.argv) and not sys.argv[_proxy_idx + 1].startswith("--")
            else "http://localhost:18080"
        )
        import os as _os
        _os.environ.setdefault("HTTP_PROXY",  _proxy_url)
        _os.environ.setdefault("HTTPS_PROXY", _proxy_url)
        _os.environ.setdefault("http_proxy",  _proxy_url)
        _os.environ.setdefault("https_proxy", _proxy_url)
    import requests

    # Display startup header
    _version = "unknown"
    if os.path.exists("version"):
        with open("version", "r") as f:
            _version = f.read().strip()
    _flags = []
    if '--no-update' in sys.argv: _flags.append('--no-update')
    if '--no-venv' in sys.argv: _flags.append('--no-venv')
    if '--no-dm' in sys.argv: _flags.append('--no-dm')
    if '--repair' in sys.argv: _flags.append('--repair')
    if '--break-system-packages' in sys.argv: _flags.append('--break-system-packages')
    if _proxy_url: _flags.append('--proxy')
    startup.header(_version, f"{sys.version_info.major}.{sys.version_info.minor}", _flags or None)
    if _proxy_url:
        startup.info(f"Routing WOS API traffic through proxy: {_proxy_url}")
        startup.info("Used for gift code redemption and player lookups.")

    # Check for mutually exclusive flags
    mutually_exclusive_flags = ["--autoupdate", "--no-update", "--repair"]
    active_flags = [flag for flag in mutually_exclusive_flags if flag in sys.argv]
    
    if len(active_flags) > 1:
        startup.error_box(
            "Invalid Arguments",
            f"{' and '.join(active_flags)} flags are mutually exclusive.\n"
            "--autoupdate: automatically install updates without prompting\n"
            "--no-update: skip all update checks\n"
            "--repair: force reinstall/repair missing or corrupted files"
        )
        sys.exit(1)

    def restart_bot():
        python = sys.executable
        script_path = os.path.abspath(sys.argv[0])
        # Filter out --no-venv and --repair from restart args to avoid loops
        filtered_args = [arg for arg in sys.argv[1:] if arg not in ["--no-venv", "--repair"]]
        args = [python, script_path] + filtered_args

        if sys.platform == "win32":
            # For Windows, provide direct venv command like initial setup
            venv_path = "bot_venv"
            venv_python_name = os.path.join(venv_path, "Scripts", "python.exe")
            startup.venv_exists_instructions(venv_python_name, sys.platform)
            sys.exit(0)
        else:
            # For non-Windows, try automatic restart
            print("  Restarting bot...")
            try:
                subprocess.Popen(args)
                os._exit(0)
            except Exception as e:
                print(f"Error restarting: {e}")
                os.execl(python, python, script_path, *sys.argv[1:])
            
    def install_packages(requirements_txt_path: str, debug: bool = False) -> bool:
        """Install packages from requirements.txt file using pip install -r."""
        full_command = [sys.executable, "-m", "pip", "install", "-r", requirements_txt_path, "--no-cache-dir"]
        
        if break_system_packages_arg():
            full_command.append("--break-system-packages")
        
        try:
            if debug:
                subprocess.check_call(full_command, timeout=1200)
            else:
                subprocess.check_call(full_command, timeout=1200, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            return True
        except Exception as e:
            if debug:
                print(f"Failed to install requirements: {e}")
            return False
    
    async def check_and_update_files():
        beta_mode = "--beta" in sys.argv
        repair_mode = "--repair" in sys.argv
        release_info = get_latest_release_info(beta_mode=beta_mode)
        
        if release_info:
            latest_tag = release_info["tag_name"]
            source_name = release_info["source"]
            
            # Check current version
            if repair_mode:
                print(f"  Repair mode: Forcing reinstall from {latest_tag}")
                current_version = "repair-mode"  # Force update in repair mode
            elif os.path.exists("version"):
                with open("version", "r") as f:
                    current_version = f.read().strip()
            else:
                current_version = "v0.0.0"

            if current_version != latest_tag or repair_mode:
                if repair_mode:
                    print(f"  Repairing installation using: {latest_tag} (from {source_name})")
                    print("  This will overwrite existing files and restore any missing components.")
                else:
                    startup.update_available(latest_tag, source_name)
                    print(f"  Update Notes: {release_info['body']}")
                print()

                update = False

                if not is_container():
                    if "--autoupdate" in sys.argv or repair_mode:
                        update = True
                    else:
                        print("  Note: If your terminal is not interactive, you can use the --autoupdate argument to skip this prompt.")
                        ask = input("  Do you want to update? (y/n): ").strip().lower()
                        update = ask == "y"
                else:
                    update = True
                    
                if update:
                    # Backup requirements.txt for dependency comparison
                    if os.path.exists("requirements.txt"):
                        try:
                            shutil.copy2("requirements.txt", "requirements.old")
                        except Exception:
                            pass  # Silent backup failure

                    if os.path.exists("db") and os.path.isdir("db"):
                        db_bak_path = "db.bak"
                        if os.path.exists(db_bak_path) and os.path.isdir(db_bak_path):
                            if not safe_remove(db_bak_path): # Create a timestamped backup to avoid upgrading without first having a backup
                                db_bak_path = f"db.bak_{int(datetime.now().timestamp())}"

                        try:
                            shutil.copytree("db", db_bak_path)
                        except Exception:
                            pass  # Database backup failed, continue anyway

                    download_url = release_info["download_url"]
                    if not download_url:
                        startup.phase_fail("Update failed", details=["No download URL available for this release"])
                        return

                    startup.phase_start(f"Downloading update from {source_name}")
                    safe_remove("package.zip")
                    download_resp = requests.get(download_url, timeout=600)
                    
                    if download_resp.status_code == 200:
                        with open("package.zip", "wb") as f:
                            f.write(download_resp.content)
                        
                        if os.path.exists("update") and os.path.isdir("update"):
                            if not safe_remove("update"):
                                startup.phase_fail("Update failed", details=["Could not remove previous update directory"])
                                return

                        try:
                            shutil.unpack_archive("package.zip", "update", "zip")
                        except Exception as e:
                            startup.phase_fail("Update failed", details=[f"Failed to extract update package: {e}"])
                            return
                            
                        safe_remove("package.zip")
                        
                        # Find the extracted directory (GitHub/GitLab archives create a subdirectory)
                        update_dir = "update"
                        extracted_items = os.listdir(update_dir)
                        if len(extracted_items) == 1 and os.path.isdir(os.path.join(update_dir, extracted_items[0])):
                            update_dir = os.path.join(update_dir, extracted_items[0])
                        
                        # Handle main.py update
                        main_py_path = os.path.join(update_dir, "main.py")
                        if os.path.exists(main_py_path):
                            safe_remove("main.py.bak")
                                
                            try:
                                if os.path.exists("main.py"):
                                    os.rename("main.py", "main.py.bak")
                            except Exception:
                                # If backup fails, just remove the current file
                                safe_remove("main.py")

                            try:
                                shutil.copy2(main_py_path, "main.py")
                            except Exception as e:
                                startup.phase_fail("Update failed", details=[f"Could not install new main.py: {e}"])
                                return
                            
                        requirements_path = os.path.join(update_dir, "requirements.txt")
                        if os.path.exists(requirements_path):
                            success = install_packages(requirements_path, debug="--verbose" in sys.argv or "--debug" in sys.argv)

                            if success:
                                # Copy new requirements.txt to working directory before cleanup
                                try:
                                    if os.path.exists("requirements.txt"):
                                        safe_remove("requirements.txt", is_dir=False)
                                    shutil.copy2(requirements_path, "requirements.txt")
                                except Exception:
                                    pass  # Silent requirements.txt copy failure

                                # Now cleanup removed packages (comparing old vs new)
                                cleanup_removed_packages()
                            else:
                                startup.phase_fail("Update failed", details=["Failed to install requirements"])
                                return
                            
                            # Remove the requirements.txt from update folder after copying
                            safe_remove(requirements_path)
                            
                        for root, _, files in os.walk(update_dir):
                            for file in files:
                                if file == "main.py":
                                    continue
                                    
                                src_path = os.path.join(root, file)
                                rel_path = os.path.relpath(src_path, update_dir)
                                dst_path = os.path.join(".", rel_path)
                                
                                # Skip certain files that shouldn't be overwritten
                                if file in ["bot_token.txt", "version"] or dst_path.startswith("db/") or dst_path.startswith("db\\"):
                                    continue
                                
                                os.makedirs(os.path.dirname(dst_path), exist_ok=True)

                                # Only backup cogs Python files (.py extension)
                                norm_path = dst_path.replace("\\", "/")
                                is_cogs_file = (norm_path.startswith("cogs/") or norm_path.startswith("./cogs/")) and file.endswith(".py")
                                
                                if is_cogs_file and os.path.exists(dst_path):
                                    # Calculate file hashes to check if backup is needed
                                    src_hash = calculate_file_hash(src_path)
                                    dst_hash = calculate_file_hash(dst_path)
                                    
                                    if src_hash != dst_hash:
                                        # Files are different, create backup
                                        cogs_bak_dir = "cogs.bak"
                                        os.makedirs(cogs_bak_dir, exist_ok=True)
                                        
                                        # Get relative path within cogs directory
                                        rel_path_in_cogs = os.path.relpath(dst_path, "cogs")
                                        backup_path = os.path.join(cogs_bak_dir, rel_path_in_cogs)
                                        
                                        # Create subdirectories in backup if needed
                                        os.makedirs(os.path.dirname(backup_path), exist_ok=True)
                                        
                                        try:
                                            # Remove old backup if exists
                                            if os.path.exists(backup_path):
                                                safe_remove(backup_path, is_dir=False)
                                            # Copy current file to backup
                                            shutil.copy2(dst_path, backup_path)
                                        except Exception:
                                            pass  # Silent backup failure

                                try:
                                    shutil.copy2(src_path, dst_path)
                                except Exception:
                                    pass  # Silent copy failure
                        
                        safe_remove("update")

                        with open("version", "w") as f:
                            f.write(latest_tag)

                        startup.phase_ok(f"Update completed (v{latest_tag} from {source_name})")

                        restart_bot()
                    else:
                        startup.phase_fail("Update failed", details=[f"HTTP {download_resp.status_code} from {source_name}"])
                        return
            else:
                startup.up_to_date(current_version, source_name)
        else:
            startup.phase_fail("Update check failed", details=["Could not fetch release info from any source"])
        
    import asyncio
    from datetime import datetime
            
    # Handle update/repair logic
    if "--repair" in sys.argv:
        asyncio.run(check_and_update_files())
    elif "--no-update" in sys.argv:
        startup.phase_ok("Update check skipped")
    else:
        asyncio.run(check_and_update_files())
            
    import discord
    from discord.ext import commands
    import sqlite3
    import logging
    import logging.handlers

    def setup_logging():
        """Configure centralized logging with category-based file handlers."""
        log_dir = 'log'
        os.makedirs(log_dir, exist_ok=True)

        # Common formatter
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        simple_formatter = logging.Formatter('%(asctime)s - %(message)s')

        # Category mappings: logger name prefix -> log file
        categories = {
            'alliance': 'alliance.txt',
            'gift': 'gift.txt',
            'redemption': 'redemption.txt',
            'notification': 'notification.txt',
            'bot': 'bot.txt',
        }

        # Create rotating file handlers for each category
        handlers = {}
        for category, filename in categories.items():
            handler = logging.handlers.RotatingFileHandler(
                os.path.join(log_dir, filename),
                maxBytes=2 * 1024 * 1024,  # 2MB
                backupCount=1,
                encoding='utf-8'
            )
            # Use simple formatter for redemption log
            if category == 'redemption':
                handler.setFormatter(simple_formatter)
            else:
                handler.setFormatter(formatter)
            handler.setLevel(logging.INFO)
            handlers[category] = handler

        # Configure loggers for each category
        for category, handler in handlers.items():
            logger = logging.getLogger(category)
            logger.setLevel(logging.INFO)
            logger.propagate = False
            if not logger.hasHandlers():
                logger.addHandler(handler)

        # Configure root logger with console output only
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.ERROR)  # Only show errors on console

    setup_logging()

    # Silence tqdm progress bars (RapidOCR's model downloads emit them
    # straight to stderr, which logging can't intercept).
    os.environ.setdefault('TQDM_DISABLE', '1')

    # Route RapidOCR / onnxruntime chatter to log/rapidocr.txt instead of
    # the console. Those libraries attach their own StreamHandlers at
    # import time, which bypass propagate/level on the parent logger;
    # we have to clear those handlers explicitly before attaching ours.
    rapidocr_log_path = os.path.join('log', 'rapidocr.txt')
    rapidocr_handler = logging.handlers.RotatingFileHandler(
        rapidocr_log_path, maxBytes=2 * 1024 * 1024, backupCount=1, encoding='utf-8',
    )
    rapidocr_handler.setFormatter(
        logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    )
    rapidocr_handler.setLevel(logging.INFO)
    for noisy_logger in ['RapidOCR', 'rapidocr', 'rapidocr.main', 'rapidocr.base',
                         'rapidocr.download_file', 'onnxruntime']:
        _noisy = logging.getLogger(noisy_logger)
        for h in list(_noisy.handlers):
            _noisy.removeHandler(h)
        _noisy.setLevel(logging.INFO)
        _noisy.propagate = False
        _noisy.addHandler(rapidocr_handler)

    # Stdout filter: redirect tagged print() calls from cogs to log files
    class _ConsoleFilter:
        PATTERNS = ['[ERROR]', '[WARNING]', '[INFO]', '[SYNC]', '[ORPHAN CHECK]',
                    '[AUTO-DISABLE]', '[MONITOR]', '[RapidOCR]']

        def __init__(self, original):
            self._original = original
            self._logger = logging.getLogger('bot')

        def write(self, text):
            stripped = text.strip()
            if stripped and any(stripped.startswith(p) for p in self.PATTERNS):
                self._logger.info(stripped)
            else:
                self._original.write(text)

        def flush(self):
            self._original.flush()

        def __getattr__(self, name):
            return getattr(self._original, name)

    sys.stdout = _ConsoleFilter(sys.stdout)

    class CustomBot(commands.Bot):
        async def on_error(self, event_name, *args, **kwargs):
            if event_name == "on_interaction":
                error = sys.exc_info()[1]
                if isinstance(error, discord.NotFound) and error.code == 10062:
                    return
            
            await super().on_error(event_name, *args, **kwargs)

        async def on_command_error(self, ctx, error):
            if isinstance(error, discord.NotFound) and error.code == 10062:
                return
            await super().on_command_error(ctx, error)

    intents = discord.Intents.default()
    intents.message_content = True

    bot = CustomBot(command_prefix="/", intents=intents)
    bot.no_dm = '--no-dm' in sys.argv

    # Captcha image saving (dev/debug only)
    # --save-captcha=1 (failed only), =2 (success only), =3 (all)
    bot.save_captcha = 0
    for arg in sys.argv:
        if arg.startswith('--save-captcha='):
            try:
                bot.save_captcha = int(arg.split('=', 1)[1])
                if bot.save_captcha not in (0, 1, 2, 3):
                    print(f"Invalid --save-captcha value: {bot.save_captcha}. Must be 0-3. Defaulting to 0.")
                    bot.save_captcha = 0
            except ValueError:
                print("Invalid --save-captcha value. Must be 0-3. Defaulting to 0.")
                bot.save_captcha = 0

    init(autoreset=True)

    token_file = "bot_token.txt"
    if not os.path.exists(token_file):
        bot_token = input("Enter the bot token: ")
        with open(token_file, "w") as f:
            f.write(bot_token)
    else:
        with open(token_file, "r") as f:
            bot_token = f.read().strip()

    if not os.path.exists("db"):
        os.makedirs("db")

    databases = {
        "conn_alliance": "db/alliance.sqlite",
        "conn_giftcode": "db/giftcode.sqlite",
        "conn_changes": "db/changes.sqlite",
        "conn_users": "db/users.sqlite",
        "conn_settings": "db/settings.sqlite",
    }

    connections = {name: sqlite3.connect(path) for name, path in databases.items()}

    def create_tables():
        with connections["conn_changes"] as conn_changes:
            conn_changes.execute("""CREATE TABLE IF NOT EXISTS nickname_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                fid INTEGER, 
                old_nickname TEXT, 
                new_nickname TEXT, 
                change_date TEXT
            )""")
            
            conn_changes.execute("""CREATE TABLE IF NOT EXISTS furnace_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT, 
                fid INTEGER, 
                old_furnace_lv INTEGER, 
                new_furnace_lv INTEGER, 
                change_date TEXT
            )""")

        with connections["conn_settings"] as conn_settings:
            conn_settings.execute("""CREATE TABLE IF NOT EXISTS botsettings (
                id INTEGER PRIMARY KEY,
                channelid INTEGER,
                giftcodestatus TEXT
            )""")

            conn_settings.execute("""CREATE TABLE IF NOT EXISTS admin (
                id INTEGER PRIMARY KEY,
                is_initial INTEGER
            )""")

            conn_settings.execute("""CREATE TABLE IF NOT EXISTS process_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                action TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'queued',
                priority INTEGER NOT NULL,
                alliance_id INTEGER,
                details TEXT NOT NULL DEFAULT '{}',
                created_at TEXT NOT NULL,
                completed_at TEXT
            )""")
            conn_settings.execute("""CREATE INDEX IF NOT EXISTS idx_process_queue_status_priority
                ON process_queue(status, priority, id)""")

        with connections["conn_users"] as conn_users:
            conn_users.execute("""CREATE TABLE IF NOT EXISTS users (
                fid INTEGER PRIMARY KEY, 
                nickname TEXT, 
                furnace_lv INTEGER DEFAULT 0, 
                kid INTEGER, 
                stove_lv_content TEXT, 
                alliance TEXT
            )""")

        with connections["conn_giftcode"] as conn_giftcode:
            conn_giftcode.execute("""CREATE TABLE IF NOT EXISTS gift_codes (
                giftcode TEXT PRIMARY KEY, 
                date TEXT
            )""")
            
            conn_giftcode.execute("""CREATE TABLE IF NOT EXISTS user_giftcodes (
                fid INTEGER, 
                giftcode TEXT, 
                status TEXT, 
                PRIMARY KEY (fid, giftcode),
                FOREIGN KEY (giftcode) REFERENCES gift_codes (giftcode)
            )""")

        with connections["conn_alliance"] as conn_alliance:
            conn_alliance.execute("""CREATE TABLE IF NOT EXISTS alliancesettings (
                alliance_id INTEGER PRIMARY KEY, 
                channel_id INTEGER, 
                interval INTEGER
            )""")
            
            conn_alliance.execute("""CREATE TABLE IF NOT EXISTS alliance_list (
                alliance_id INTEGER PRIMARY KEY, 
                name TEXT
            )""")

    create_tables()
    startup.phase_ok("Database ready")

    async def load_cogs():
        cogs = ["pimp_my_bot", "process_queue", "bot_main_menu", "alliance_sync", "alliance", "alliance_member_operations", "bot_operations", "alliance_logs", "bot_support", "bot_health", "gift_operations", "alliance_history", "alliance_w_command", "bot_startup", "notification_system", "notification_schedule", "alliance_id_channel", "bot_backup", "notification_editor", "notification_templates", "notification_wizard", "attendance", "attendance_report", "minister_schedule", "minister_menu", "minister_archive", "alliance_registration", "bear_track", "attendance_ocr"]

        failed_cogs = []

        # Suppress all console output during cog loading to prevent
        # third-party libraries (RapidOCR, onnxruntime) from spamming.
        logging.disable(logging.INFO)
        _real_stderr = sys.stderr
        sys.stderr = open(os.devnull, 'w')
        # Also suppress empty newlines leaking through stdout filter
        _real_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

        for cog in cogs:
            try:
                await bot.load_extension(f"cogs.{cog}")
            except Exception as e:
                failed_cogs.append((cog, str(e)))

        sys.stdout = _real_stdout
        sys.stderr = _real_stderr
        logging.disable(logging.NOTSET)

        total = len(cogs)
        loaded = total - len(failed_cogs)
        if failed_cogs:
            startup.phase_fail(
                f"{loaded}/{total} modules loaded",
                details=[f"{cog}: {error}" for cog, error in failed_cogs],
                fix="python main.py --repair"
            )
        else:
            startup.phase_ok(f"{loaded}/{total} modules loaded")

    @bot.event
    async def on_ready():
        try:
            startup.phase_ok(f"Connected to Discord as {bot.user}")
            await bot.tree.sync()

            # API health checks
            try:
                import aiohttp as _aio
                timeout = _aio.ClientTimeout(total=5)
                async with _aio.ClientSession(timeout=timeout) as session:
                    try:
                        headers = {'X-API-Key': 'super_secret_bot_token_nobody_will_ever_find'}
                        async with session.get("http://gift-code-api.whiteout-bot.com/giftcode_api.php", headers=headers) as resp:
                            if resp.status < 500:
                                startup.api_status("Gift Code Distribution API", "ok")
                            else:
                                startup.api_status("Gift Code Distribution API", "error", f"HTTP {resp.status}")
                    except Exception:
                        startup.api_status("Gift Code Distribution API", "error", "Offline")

                try:
                    proxy_detail = (
                        f"via proxy {os.environ.get('HTTPS_PROXY')}"
                        if os.environ.get("HTTPS_PROXY")
                        else "no proxy"
                    )
                    sync_cog = bot.get_cog("AllianceSync")
                    if sync_cog and hasattr(sync_cog, 'login_handler'):
                        status = await sync_cog.login_handler.check_apis_availability()
                        if status.get('api1_available') and status.get('api2_available'):
                            startup.api_status("Gift Code Redemption API", "ok", "Dual-API mode")
                        elif status.get('api1_available') or status.get('api2_available'):
                            startup.api_status("Gift Code Redemption API", "ok", "Single-API mode")
                        else:
                            startup.api_status("Gift Code Redemption API", "error", proxy_detail)
                    else:
                        startup.api_status("Gift Code Redemption API", "error", "Check failed")
                except Exception:
                    startup.api_status("Gift Code Redemption API", "error", "Check failed")
            except Exception:
                pass

            # Summary with per-alliance breakdown
            alliance_details = []
            try:
                import sqlite3 as _sq
                with _sq.connect('db/alliance.sqlite') as _c:
                    alliances = _c.execute("SELECT alliance_id, name FROM alliance_list ORDER BY name").fetchall()
                    alliance_count = len(alliances)
                with _sq.connect('db/users.sqlite') as _c:
                    member_count = _c.execute("SELECT COUNT(*) FROM users").fetchone()[0]
                    for aid, name in alliances:
                        count = _c.execute("SELECT COUNT(*) FROM users WHERE alliance = ?", (aid,)).fetchone()[0]
                        alliance_details.append((name, count))
            except Exception:
                alliance_count = 0
                member_count = 0

            startup.summary(len(bot.guilds), alliance_count, member_count, alliance_details or None)

            # Wait briefly for cog on_ready handlers to complete, then show DM status
            await asyncio.sleep(2)
            if getattr(bot, 'no_dm', False):
                startup.info("Startup DM skipped")
            elif getattr(bot, 'startup_dm_sent', False):
                startup.info("Startup DM sent to Global Admin")

            startup.ready()
        except Exception as e:
            print(f"Error syncing commands: {e}")

    async def main():
        await load_cogs()

        attempt = 0
        while True:
            attempt += 1
            try:
                await bot.start(bot_token)
                break  # Clean exit if bot.start() returns normally

            except discord.LoginFailure:
                startup.error_box("Login Failed", "Invalid bot token.", "Check your bot_token.txt file.\nGuide: https://github.com/whiteout-project/bot/wiki/Creating-a-Discord-Application")
                break

            except discord.PrivilegedIntentsRequired:
                startup.error_box("Login Failed", "Privileged intents not enabled.", "Follow steps 5+ at:\nhttps://github.com/whiteout-project/bot/wiki/Creating-a-Discord-Application")
                break

            except discord.HTTPException as e:
                if e.status == 429:
                    startup.connection_retry(attempt, "rate limited", 60)
                    await asyncio.sleep(60)
                elif e.status >= 500:
                    startup.connection_retry(attempt, f"server error (HTTP {e.status})", 30)
                    await asyncio.sleep(30)
                else:
                    startup.connection_retry(attempt, f"HTTP {e.status}", 30)
                    await asyncio.sleep(30)

            except discord.GatewayNotFound:
                startup.connection_retry(attempt, "gateway unavailable", 30)
                await asyncio.sleep(30)

            except (aiohttp.ClientConnectorDNSError, aiohttp.ClientConnectorError):
                startup.connection_retry(attempt, "connection failed", 30)
                await asyncio.sleep(30)

            except OSError as e:
                if e.errno in (-3, 11001):  # DNS errors (Linux/Windows)
                    startup.connection_retry(attempt, "DNS failed", 30)
                    await asyncio.sleep(30)
                else:
                    raise

    def run_bot():
        import signal

        async def start_bot():
            """Start the bot with proper shutdown handling."""
            stop_event = asyncio.Event()

            def signal_handler():
                if is_container():
                    print(f"\n  Received shutdown signal. Shutting down gracefully...")
                else:
                    print(f"\n  {startup.shutdown()}")
                stop_event.set()

            loop = asyncio.get_running_loop()

            # Register signal handlers
            if sys.platform != "win32":
                for sig in (signal.SIGINT, signal.SIGTERM):
                    loop.add_signal_handler(sig, signal_handler)
            else:
                # Windows doesn't support add_signal_handler, use traditional signal
                def win_handler(signum, frame):
                    signal_handler()
                signal.signal(signal.SIGINT, win_handler)

            # Start bot in background task
            bot_task = asyncio.create_task(main())

            # Wait for either bot to finish or shutdown signal
            shutdown_task = asyncio.create_task(stop_event.wait())
            done, pending = await asyncio.wait(
                [bot_task, shutdown_task],
                return_when=asyncio.FIRST_COMPLETED
            )

            # Cancel pending tasks
            for task in pending:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

            # Properly close the bot and await completion
            if not bot.is_closed():
                await bot.close()

        try:
            asyncio.run(start_bot())
        except KeyboardInterrupt:
            pass  # Already handled by signal handler

    if __name__ == "__main__":
        run_bot()