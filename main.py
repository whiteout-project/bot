import subprocess
import sys
import os
import shutil
import stat

# Python version check
MIN_PYTHON = (3, 11)
print(f"Bot running Python {sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}")

if sys.version_info < MIN_PYTHON:
    print(f"ERROR: Python {MIN_PYTHON[0]}.{MIN_PYTHON[1]}+ is required.")
    print("\nTo upgrade Python:")

    if sys.platform == "win32":
        print("  1. Download Python 3.13 or newer from: https://www.python.org/downloads/")
        print("  2. Run the installer (check 'Add Python to PATH' during installation)")
        print("  3. Delete the 'bot_venv' folder in your bot directory")
        print("  4. Run the bot again")
    elif os.path.exists("/.dockerenv"):
        print("  Update your Dockerfile to use a newer Python base image, such as:")
        print("    FROM python:3.12.12-slim-bookworm")
        print("  Then rebuild your container")
    elif os.path.exists("/var/run/secrets/kubernetes.io"):
        print("  Update your container image to use Python 3.11 or newer.")
    else:
        print("  1. Install Python 3.13 using pyenv:")
        print("       pyenv install 3.13")
        print("       pyenv local 3.13")
        print("  2. Delete the 'bot_venv' folder in your bot directory")
        print("  3. Run the bot again")

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
        print("WARNING: On linux, running without a virtual environment won't work unless you break system packages.")
        print("WARNING: Add the --break-system-packages argument to your command to confirm you understand the risks.")
        print("Exiting...")
        
        sys.exit(1)
    
    return '--no-venv' in sys.argv or is_container() or is_ci_environment()

# Handle venv setup
if sys.prefix == sys.base_prefix and not should_skip_venv():
    print("Running the bot in a venv (virtual environment) to avoid dependency conflicts.")
    print("Note: You can skip venv creation with the --no-venv argument if needed.")
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
            print("Attempting to create virtual environment automatically...")
            subprocess.check_call([sys.executable, "-m", "venv", venv_path], timeout=300)
            print(f"Virtual environment created at {venv_path}")

            if sys.platform == "win32":
                print("\nVirtual environment created.")
                print("To continue, please run the script again with the venv Python:")
                print(f"  1. Ensure CMD or PowerShell is open in this directory: {os.getcwd()}")
                print(f"  2. Run this exact command: {venv_python_name} {os.path.basename(sys.argv[0])}")
                sys.exit(0)
            else: # For non-Windows, try to relaunch automatically
                print("Restarting script in virtual environment...")
                venv_python_executable = os.path.join(venv_path, "bin", "python")
                os.execv(venv_python_executable, [venv_python_executable] + sys.argv)

        except Exception as e:
            print("Failed to create virtual environment automatically.")
            print(f"Error: {e}")
            print("Please create one manually with: python -m venv bot_venv")
            print("Then activate it and run this script again.")
            print("See also: https://docs.python.org/3/library/venv.html#how-venvs-work")
            sys.exit(1)
    else: # Venv exists
        if sys.platform == "win32":
            print(f"Virtual environment at {venv_path} exists.")
            print("To ensure you are using it, please run the script with the venv Python:")
            print(f"  1. Ensure CMD or PowerShell is open in this directory: {os.getcwd()}")
            print(f"  2. Run this exact command: {venv_python_name} {os.path.basename(sys.argv[0])}")
            sys.exit(0)
        elif '--no-venv' in sys.argv:
            print("Virtual environment setup skipped due to --no-venv flag.")
            print("Warning: Dependencies will be installed system-wide which may cause conflicts.")
        else: # For non-Windows, if venv exists but we're not in it, try to relaunch
            venv_python_executable = os.path.join(venv_path, "bin", "python")
            if os.path.exists(venv_python_executable):
                print(f"Using existing virtual environment at {venv_path}. Restarting...")
                os.execv(venv_python_executable, [venv_python_executable] + sys.argv)
            else:
                print(f"Virtual environment at {venv_path} appears corrupted.")
                print("Please remove it and run the script again, or create a new one manually.")
                sys.exit(1)

try: # Import or install requests so we can get the requirements
    import requests
except ImportError:
    print("Installing requests (required for dependency management)...")
    try:
        cmd = [sys.executable, "-m", "pip", "install", "requests"]
        
        if break_system_packages_arg():
            cmd.append("--break-system-packages")
        
        subprocess.check_call(cmd, timeout=300, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        import requests
    except Exception as e:
        print(f"Failed to install requests: {e}")
        print("Please install requests manually: pip install requests")
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
    
    print(F.YELLOW + f"Found {len(packages)} packages to remove{reason}: {', '.join(packages)}" + R)
    debug_mode = "--verbose" in sys.argv or "--debug" in sys.argv
    
    for package in packages:
        try:
            cmd = [sys.executable, "-m", "pip", "uninstall", "-y", package]
            
            if debug_mode:
                subprocess.check_call(cmd, timeout=300)
            else:
                subprocess.check_call(cmd, timeout=300, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            print(F.GREEN + f"✓ Removed {package}" + R)
        except subprocess.CalledProcessError:
            print(F.YELLOW + f"✗ Could not remove {package} (might be needed by other packages)" + R)
        except Exception as e:
            print(F.YELLOW + f"✗ Error removing {package}: {e}" + R)

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
            print(F.YELLOW + f"Error comparing requirements: {e}" + R)
    
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
    "opencv-python",
    "opencv-python-headless",
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
    except Exception as e:
        print(f"Error checking requirements.txt: {e}")
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
            print(f"Checking for updates from {source['name']}...")
            
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
            if hasattr(e, 'response') and e.response is not None:
                if e.response.status_code == 404:
                    print(f"{source['name']} repository not found or unavailable")
                elif e.response.status_code in [403, 429]:
                    print(f"{source['name']} access limited (rate limit or access denied)")
                else:
                    print(f"{source['name']} returned HTTP {e.response.status_code}")
            else:
                print(f"{source['name']} connection failed")
            continue
        except Exception as e:
            print(f"Failed to check {source['name']}: {e}")
            continue
        
    print("All update sources failed")
    return None

def download_requirements_from_release(beta_mode=False):
    """
    Download requirements.txt file directly from the latest release or main branch if beta mode.
    """
    if os.path.exists("requirements.txt"):
        return True
    
    print("Downloading requirements.txt from latest release...")
    
    # Get latest release info to find the tag
    release_info = get_latest_release_info(beta_mode=beta_mode)
    if not release_info:
        print("Could not get release information")
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
        print(f"Unknown source: {source_name}")
        return False
    
    try:
        print(f"Downloading from {source_name}: {raw_url}")
        response = requests.get(raw_url, timeout=30)
        
        if response.status_code == 200:
            with open("requirements.txt", "w") as f:
                f.write(response.text)
            print("Successfully downloaded requirements.txt")
            return True
        else:
            print(f"Failed to download: HTTP {response.status_code}")
            return False
            
    except Exception as e:
        print(f"Error downloading requirements.txt: {e}")
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
    print("Installing onnxruntime from nightly build (Python 3.14+)...")
    cmd = [
        sys.executable, "-m", "pip", "install", "--pre",
        "--extra-index-url", "https://aiinfra.pkgs.visualstudio.com/PublicPackages/_packaging/ORT-Nightly/pypi/simple/",
        "onnxruntime", "--no-cache-dir"
    ]
    if break_system_packages_arg():
        cmd.append("--break-system-packages")
    result = subprocess.run(cmd, timeout=1200, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"✗ Failed to install onnxruntime nightly: {result.stderr}")
        raise subprocess.CalledProcessError(result.returncode, cmd)
    print("✓ onnxruntime nightly installed successfully")

def install_onnxruntime_stable(version_spec="onnxruntime>=1.18.1"):
    """Install onnxruntime from stable PyPI."""
    print("Installing stable onnxruntime from PyPI...")
    cmd = [sys.executable, "-m", "pip", "install", version_spec, "--no-cache-dir", "--force-reinstall"]
    if break_system_packages_arg():
        cmd.append("--break-system-packages")
    subprocess.check_call(cmd, timeout=1200, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

def check_and_install_requirements():
    """Check each requirement and install missing ones."""
    if not os.path.exists("requirements.txt"):
        print("No requirements.txt found")
        return False
        
    # Read requirements
    with open("requirements.txt", "r") as f:
        requirements = [line.strip() for line in f if line.strip() and not line.startswith("#")]
    
    print(f"Checking {len(requirements)} requirements...")
    
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
            print(f"✗ {package_name} - MISSING")
            missing_packages.append(requirement)
    
    if missing_packages: # Install missing packages
        print(f"Installing {len(missing_packages)} missing packages...")

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
                print(f"✓ {package} installed successfully")

            except Exception as e:
                print(f"✗ Failed to install {package}: {e}")
                return False
    
    print("✓ All requirements satisfied")
    return True

def setup_dependencies(beta_mode=False):
    """Main function to set up all dependencies."""
    print("Checking dependencies...")
    
    removed_obsolete = False
    if has_obsolete_requirements():
        print("! Warning: requirements.txt contains obsolete packages from older version")
        print("! Removing outdated requirements.txt and downloading fresh copy...")
        removed_obsolete = True

        if not safe_remove("requirements.txt", is_dir=False):
            print("! Error removing obsolete requirements.txt")

    if not os.path.exists("requirements.txt"):
        if not removed_obsolete:
            print("! Warning: requirements.txt not found")
        if not download_requirements_from_release(beta_mode=beta_mode):
            print("✗ Failed to download requirements.txt")
            print("• Please download the complete bot package from: https://github.com/whiteout-project/bot/releases")
            return False
    
    if not check_and_install_requirements():
        print("✗ Failed to install requirements")
        return False
    
    return True

beta_mode = "--beta" in sys.argv
if not setup_dependencies(beta_mode=beta_mode):
    print("Warning: Dependency setup incomplete. Please update if prompted or run --repair to try fixing this.")
    print("If update or repair fails, please install manually with: pip install -r requirements.txt")

try:
    from colorama import Fore, Style, init
    import discord
    print("✓ All core imports successful")
except ImportError as e:
    print(f"Import failed even after dependency setup: {e}")
    print("Please restart the script or install dependencies manually")
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
            print(F.YELLOW + f"⚠️ Microsoft Visual C++ Redistributable ({arch}) not found!" + R)
            print(F.YELLOW + "Gift code redemption (captcha solver) will not work until this is installed." + R)
            print(F.YELLOW + f"Download and install from: " + F.CYAN + download_url + R)
            print(F.YELLOW + "Then restart the bot, and the gift code redemption should work." + R)
            print()
            return False

    except Exception:
        return True  # If we can't check, we hope it's fine

def startup_cleanup():
    """Perform all cleanup tasks on startup - directories, files, and legacy packages."""
    v1_path = "V1oldbot"
    if os.path.exists(v1_path) and safe_remove(v1_path):
        print(f"Removed directory: {v1_path}")
    
    v2_path = "V2Old"
    if os.path.exists(v2_path) and safe_remove(v2_path):
        print(f"Removed directory: {v2_path}")
    
    pictures_path = "pictures"
    if os.path.exists(pictures_path) and safe_remove(pictures_path):
        print(f"Removed directory: {pictures_path}")
    
    txt_path = "autoupdateinfo.txt"
    if os.path.exists(txt_path) and safe_remove(txt_path):
        print(f"Removed file: {txt_path}")
    
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
        
        print(F.GREEN + "Applied SSL context patch using certifi for default HTTPS connections." + R)
    else: # Assume if it's already patched, it's for a good reason, just log it.
        print(F.YELLOW + "SSL default HTTPS context seems to be already modified. Skipping certifi patch." + R)
except ImportError:
    print(F.RED + "Certifi library not found. SSL certificate verification might fail until it's installed." + R)
except Exception as e:
    print(F.RED + f"Error applying SSL context patch: {e}" + R)

if __name__ == "__main__":
    import requests

    # Check for mutually exclusive flags
    mutually_exclusive_flags = ["--autoupdate", "--no-update", "--repair"]
    active_flags = [flag for flag in mutually_exclusive_flags if flag in sys.argv]
    
    if len(active_flags) > 1:
        print(F.RED + f"Error: {' and '.join(active_flags)} flags are mutually exclusive." + R)
        print("Use --autoupdate to automatically install updates without prompting.")
        print("Use --no-update to skip all update checks.")
        print("Use --repair to force reinstall/repair missing or corrupted files.")
        sys.exit(1)

    def restart_bot():
        python = sys.executable
        script_path = os.path.abspath(sys.argv[0])
        # Filter out --no-venv and --repair from restart args to avoid loops
        filtered_args = [arg for arg in sys.argv[1:] if arg not in ["--no-venv", "--repair"]]
        args = [python, script_path] + filtered_args

        if sys.platform == "win32":
            # For Windows, provide direct venv command like initial setup
            print(F.YELLOW + "Please restart the bot manually to continue:" + R)
            print(F.CYAN + f"  1. Ensure CMD or PowerShell is open in this directory: {os.getcwd()}" + R)
            
            venv_path = "bot_venv"
            venv_python_name = os.path.join(venv_path, "Scripts", "python.exe")
            print(F.CYAN + "  2. Run this exact command: " + F.GREEN + f"{venv_python_name} {os.path.basename(script_path)}" + R)
            sys.exit(0)
        else:
            # For non-Windows, try automatic restart
            print(F.YELLOW + "Restarting bot..." + R)
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
                print(F.YELLOW + f"Repair mode: Forcing reinstall from {latest_tag}" + R)
                current_version = "repair-mode"  # Force update in repair mode
            elif os.path.exists("version"):
                with open("version", "r") as f:
                    current_version = f.read().strip()
                if beta_mode:
                    print(F.YELLOW + "Beta mode: Comparing latest commit from main branch" + R)
            else:
                current_version = "v0.0.0"
                if beta_mode:
                    print(F.YELLOW + "Beta mode: Comparing latest commit from main branch" + R)

            if not repair_mode:
                print(F.CYAN + f"Current version: {current_version}" + R)

            if current_version != latest_tag or repair_mode:
                if repair_mode:
                    print(F.YELLOW + f"Repairing installation using: {latest_tag} (from {source_name})" + R)
                    print("This will overwrite existing files and restore any missing components.")
                else:
                    print(F.YELLOW + f"New version available: {latest_tag} (from {source_name})" + R)
                    print("Update Notes:")
                    print(release_info["body"])
                print()
                
                update = False
                
                if not is_container():
                    if "--autoupdate" in sys.argv or repair_mode:
                        update = True
                    else:
                        print("Note: If your terminal is not interactive, you can use the --autoupdate argument to skip this prompt.")
                        ask = input("Do you want to update? (y/n): ").strip().lower()
                        update = ask == "y"
                else:
                    print(F.YELLOW + "Running in a container. Skipping update prompt." + R)
                    update = True
                    
                if update:
                    # Backup requirements.txt for dependency comparison
                    if os.path.exists("requirements.txt"):
                        try:
                            shutil.copy2("requirements.txt", "requirements.old")
                        except Exception as e:
                            print(F.YELLOW + f"Could not backup requirements.txt: {e}" + R)
                    
                    if os.path.exists("db") and os.path.isdir("db"):
                        print(F.YELLOW + "Making backup of database..." + R)
                        
                        db_bak_path = "db.bak"
                        if os.path.exists(db_bak_path) and os.path.isdir(db_bak_path):
                            if not safe_remove(db_bak_path): # Create a timestamped backup to avoid upgrading without first having a backup
                                db_bak_path = f"db.bak_{int(datetime.now().timestamp())}"
                                print(F.YELLOW + "WARNING: Couldn't remove db.bak folder. Making backup with timestamp instead." + R)

                        try:
                            shutil.copytree("db", db_bak_path)
                            print(F.GREEN + f"Backup completed: db → {db_bak_path}" + R)
                        except Exception as e:
                            print(F.RED + f"WARNING: Failed to create database backup: {e}" + R)
                                            
                    download_url = release_info["download_url"]
                    if not download_url:
                        print(F.RED + "No download URL available for this release" + R)
                        return
                        
                    print(F.YELLOW + f"Downloading update from {source_name}..." + R)
                    safe_remove("package.zip")
                    download_resp = requests.get(download_url, timeout=600)
                    
                    if download_resp.status_code == 200:
                        with open("package.zip", "wb") as f:
                            f.write(download_resp.content)
                        
                        if os.path.exists("update") and os.path.isdir("update"):
                            if not safe_remove("update"):
                                print(F.RED + "WARNING: Could not remove previous update directory" + R)
                                return
                            
                        try:
                            shutil.unpack_archive("package.zip", "update", "zip")
                        except Exception as e:
                            print(F.RED + f"ERROR: Failed to extract update package: {e}" + R)
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
                            except Exception as e:
                                print(F.YELLOW + f"Could not backup main.py: {e}" + R)
                                # If backup fails, just remove the current file
                                if safe_remove("main.py"):
                                    print(F.YELLOW + "Removed current main.py" + R)
                                else:
                                    print(F.RED + "Warning: Could not backup or remove current main.py" + R)
                            
                            try:
                                shutil.copy2(main_py_path, "main.py")
                            except Exception as e:
                                print(F.RED + f"ERROR: Could not install new main.py: {e}" + R)
                                return
                            
                        requirements_path = os.path.join(update_dir, "requirements.txt")
                        if os.path.exists(requirements_path):                      
                            print(F.YELLOW + "Installing any new requirements..." + R)
                            
                            success = install_packages(requirements_path, debug="--verbose" in sys.argv or "--debug" in sys.argv)
                            
                            if success:
                                print(F.GREEN + "New requirements installed." + R)
                                
                                # Copy new requirements.txt to working directory before cleanup
                                try:
                                    if os.path.exists("requirements.txt"):
                                        safe_remove("requirements.txt", is_dir=False)
                                    shutil.copy2(requirements_path, "requirements.txt")
                                    print(F.GREEN + "Updated requirements.txt" + R)
                                except Exception as e:
                                    print(F.YELLOW + f"Warning: Could not update requirements.txt: {e}" + R)
                                
                                # Now cleanup removed packages (comparing old vs new)
                                cleanup_removed_packages()
                            else:
                                print(F.RED + "Failed to install requirements." + R)
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
                                        except Exception as e:
                                            print(F.YELLOW + f"Could not create backup of {dst_path}: {e}" + R)
                                        
                                try:
                                    shutil.copy2(src_path, dst_path)
                                except Exception as e:
                                    print(F.RED + f"Failed to copy {file} to {dst_path}: {e}" + R)
                        
                        if not safe_remove("update"):
                            print(F.RED + "WARNING: update folder could not be removed. You may want to remove it manually." + R)
                        
                        with open("version", "w") as f:
                            f.write(latest_tag)
                        
                        print(F.GREEN + f"Update completed successfully from {source_name}." + R)
                        
                        restart_bot()
                    else:
                        print(F.RED + f"Failed to download the update from {source_name}. HTTP status: {download_resp.status_code}" + R)
                        return  
        else:
            print(F.RED + "Failed to fetch latest release info from all sources" + R)
        
    import asyncio
    from datetime import datetime
            
    # Handle update/repair logic
    if "--repair" in sys.argv:
        asyncio.run(check_and_update_files())
    elif "--no-update" in sys.argv:
        print(F.YELLOW + "Update check skipped due to --no-update flag." + R)
    else:
        asyncio.run(check_and_update_files())
            
    import discord
    from discord.ext import commands
    import sqlite3

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
        
        print(F.GREEN + "db folder created" + R)

    databases = {
        "conn_alliance": "db/alliance.sqlite",
        "conn_giftcode": "db/giftcode.sqlite",
        "conn_changes": "db/changes.sqlite",
        "conn_users": "db/users.sqlite",
        "conn_settings": "db/settings.sqlite",
    }

    connections = {name: sqlite3.connect(path) for name, path in databases.items()}

    print(F.GREEN + "Database connections have been successfully established." + R)

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

        print(F.GREEN + "All tables checked." + R)

    create_tables()

    async def load_cogs():
        cogs = ["pimp_my_bot", "olddb", "control", "alliance", "alliance_member_operations", "bot_operations", "logsystem", "support_operations", "gift_operations", "changes", "w", "wel", "other_features", "bear_trap", "bear_trap_schedule", "id_channel", "backup_operations", "bear_trap_editor", "bear_trap_templates", "bear_trap_wizard", "attendance", "attendance_report", "minister_schedule", "minister_menu", "minister_archive", "registration"]

        failed_cogs = []
        
        for cog in cogs:
            try:
                await bot.load_extension(f"cogs.{cog}")
            except Exception as e:
                print(f"✗ Failed to load cog {cog}: {e}")
                failed_cogs.append(cog)
        
        if failed_cogs:
            print(F.RED + f"\n⚠️  {len(failed_cogs)} cog(s) failed to load:" + R)
            for cog in failed_cogs:
                print(F.YELLOW + f"   • {cog}" + R)
            print(F.YELLOW + "\nThe bot will continue with reduced functionality." + R)
            print(F.YELLOW + "To fix missing or corrupted files, run: " + F.GREEN + "python main.py --repair" + R)
            print(F.YELLOW + "This will download and restore all files from the latest release.\n" + R)

    @bot.event
    async def on_ready():
        try:
            print(f"{F.GREEN}Logged in as {F.CYAN}{bot.user}{R}")
            await bot.tree.sync()
        except Exception as e:
            print(f"Error syncing commands: {e}")

    async def main():
        await load_cogs()

        while True:
            try:
                await bot.start(bot_token)
                break  # Clean exit if bot.start() returns normally

            except discord.LoginFailure:
                print(f"\n{F.RED}Login failed: Invalid bot token!{R}")
                print(f"{F.YELLOW}Ensure your bot_token.txt file contains the proper token.{R}")
                print(f"{F.YELLOW}If necessary, you can reset it via steps 3-4 here:{R}")
                print(f"{F.YELLOW}https://github.com/whiteout-project/bot/wiki/Creating-a-Discord-Application{R}")
                break

            except discord.PrivilegedIntentsRequired:
                print(f"\n{F.RED}Login failed: Privileged intents not enabled.{R}")
                print(f"{F.YELLOW}Follow steps 5 onwards to enable the required intents:{R}")
                print(f"{F.YELLOW}https://github.com/whiteout-project/bot/wiki/Creating-a-Discord-Application{R}")
                break

            except discord.HTTPException as e:
                if e.status == 429:
                    print(f"\n{F.YELLOW}Rate limited by Discord (HTTP 429).{R}")
                    print(f"{F.YELLOW}This often happens on shared nodes with many Discord bots.{R}")
                    print(f"{F.YELLOW}Possible solutions:{R}")
                    print(f"{F.YELLOW}  - Just wait it out and retry (retrying in 60 seconds){R}")
                    print(f"{F.YELLOW}  - If hosted on a VPS, move your bot to a different node{R}")
                    print(f"{F.YELLOW}  - Use a different hosting provider (last resort){R}")
                    await asyncio.sleep(60)
                elif e.status >= 500:
                    print(f"\n{F.YELLOW}Discord server error (HTTP {e.status}).{R}")
                    print(f"{F.YELLOW}Discord may be experiencing issues. Retrying in 30 seconds...{R}")
                    await asyncio.sleep(30)
                else:
                    print(f"\n{F.RED}Discord HTTP error (HTTP {e.status}): {e.text}{R}")
                    print(f"{F.YELLOW}Retrying in 30 seconds...{R}")
                    await asyncio.sleep(30)

            except discord.GatewayNotFound:
                print(f"\n{F.YELLOW}Discord gateway unavailable.{R}")
                print(f"{F.YELLOW}Discord may be experiencing issues. Retrying in 30 seconds...{R}")
                await asyncio.sleep(30)

            except (aiohttp.ClientConnectorDNSError, aiohttp.ClientConnectorError):
                print(f"\n{F.YELLOW}Connection issue: Unable to reach Discord servers.{R}")
                print(f"{F.YELLOW}Possible causes:{R}")
                print(f"{F.YELLOW}  - Internet connection is down{R}")
                print(f"{F.YELLOW}  - DNS resolution failed{R}")
                print(f"{F.YELLOW}  - Discord is blocked by firewall/ISP{R}")
                print(f"{F.YELLOW}  - VPS network issues{R}")
                print(f"{F.YELLOW}Retrying in 30 seconds...{R}")
                await asyncio.sleep(30)

            except OSError as e:
                if e.errno in (-3, 11001):  # DNS errors (Linux/Windows)
                    print(f"\n{F.YELLOW}Connection issue: DNS resolution failed.{R}")
                    print(f"{F.YELLOW}Check your DNS settings or try different DNS servers.{R}")
                    print(f"{F.YELLOW}Retrying in 30 seconds...{R}")
                    await asyncio.sleep(30)
                else:
                    raise

    def run_bot():
        import signal

        def get_shutdown_message():
            import random
            try:
                from cogs.pimp_my_bot import theme
                shutdown_messages = [
                    f"{theme.shutdownStopIcon} Ctrl+C detected! The bot is powering down... beep boop!",
                    f"{theme.shutdownHandIcon} Caught Ctrl+C! Time for the bot to take a nap. Sweet dreams!",
                    f"{theme.shutdownPlugIcon} Ctrl+C pressed! Unplugging the bot. See you next time!",
                    f"{theme.shutdownDoorIcon} Exit signal received! The bot has left the building...",
                    f"{theme.shutdownZzzIcon} Ctrl+C! The bot is going to sleep. Wake me up when you need me!",
                    f"{theme.shutdownClapperIcon} And that's a wrap! Bot shutting down gracefully.",
                    f"{theme.shutdownMoonIcon} Trying to turn the bot off and not on again. Ctrl+C ya later!",
                    f"{theme.shutdownSparkleIcon} Ctrl+C and poof! The bot vanishes into thin air...",
                ]
            except Exception:
                # Fallback if theme not loaded yet
                shutdown_messages = [
                    "🛑 Ctrl+C detected! The bot is powering down... beep boop!",
                    "👋 Caught Ctrl+C! Time for the bot to take a nap. Sweet dreams!",
                ]
            return random.choice(shutdown_messages)

        async def start_bot():
            """Start the bot with proper shutdown handling."""
            stop_event = asyncio.Event()

            def signal_handler():
                if is_container():
                    print(f"\n{F.YELLOW}Received shutdown signal. Shutting down gracefully...{R}")
                else:
                    print(f"\n{F.YELLOW}{get_shutdown_message()}{R}")
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