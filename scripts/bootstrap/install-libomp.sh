#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
OPENMP_VERSION="${OPENMP_VERSION:-16.0.6}"

if [[ "$(uname -s)" != "Darwin" ]]; then
  exit 0
fi

arch="$(uname -m)"
macos_major="$(sw_vers -productVersion | cut -d. -f1)"

if [[ "${arch}" == "arm64" ]]; then
  libomp_prefix="/opt/homebrew/opt/libomp"
else
  libomp_prefix="/usr/local/opt/libomp"
fi

lightgbm_importable() {
  local python_bin="${REPO_ROOT}/.venv/bin/python"
  if [[ ! -x "${python_bin}" ]]; then
    return 1
  fi

  "${python_bin}" - <<'PY' >/dev/null 2>&1
import lightgbm
print(lightgbm.__version__)
PY
}

brew_bottle_tag() {
  local codename
  case "${macos_major}" in
    14) codename="sonoma" ;;
    15) codename="sequoia" ;;
    16) codename="tahoe" ;;
    *) return 1 ;;
  esac

  if [[ "${arch}" == "arm64" ]]; then
    printf 'arm64_%s\n' "${codename}"
  else
    printf '%s\n' "${codename}"
  fi
}

brew_has_matching_bottle() {
  if ! command -v brew >/dev/null 2>&1; then
    return 1
  fi

  local tag
  tag="$(brew_bottle_tag)" || return 1
  HOMEBREW_NO_AUTO_UPDATE=1 brew info --json=v2 libomp | grep -q "\"${tag}\":"
}

ensure_cmake() {
  local venv_cmake="${REPO_ROOT}/.venv/bin/cmake"
  if [[ -x "${venv_cmake}" ]]; then
    printf '%s\n' "${venv_cmake}"
    return 0
  fi

  if command -v cmake >/dev/null 2>&1; then
    command -v cmake
    return 0
  fi

  if [[ ! -x "${REPO_ROOT}/.venv/bin/python" ]]; then
    echo "Project virtualenv is missing; run 'uv sync' before installing libomp." >&2
    return 1
  fi

  echo "Installing cmake<4 into the project environment for libomp source build..."
  uv pip install --python "${REPO_ROOT}/.venv/bin/python" 'cmake<4' >/dev/null
  printf '%s\n' "${venv_cmake}"
}

build_libomp_from_source() {
  local cmake_bin
  cmake_bin="$(ensure_cmake)"

  local workdir archive src_dir build_dir jobs
  workdir="$(mktemp -d "${TMPDIR:-/tmp}/libomp-build.XXXXXX")"
  archive="${workdir}/openmp-${OPENMP_VERSION}.src.tar.xz"
  src_dir="${workdir}/openmp-${OPENMP_VERSION}.src"
  build_dir="${workdir}/build"
  jobs="$(sysctl -n hw.ncpu 2>/dev/null || echo 4)"

  trap 'rm -rf "${workdir}"' RETURN

  echo "Building libomp ${OPENMP_VERSION} from source for macOS ${macos_major} ${arch}..."
  curl -fsSL -L "https://github.com/llvm/llvm-project/releases/download/llvmorg-${OPENMP_VERSION}/openmp-${OPENMP_VERSION}.src.tar.xz" -o "${archive}"
  tar -xf "${archive}" -C "${workdir}"

  mkdir -p "${libomp_prefix}"
  "${cmake_bin}" \
    -S "${src_dir}" \
    -B "${build_dir}" \
    -DCMAKE_BUILD_TYPE=Release \
    -DCMAKE_INSTALL_PREFIX="${libomp_prefix}" \
    -DCMAKE_MODULE_PATH="${REPO_ROOT}/scripts/bootstrap/cmake" \
    -DLIBOMP_INSTALL_ALIASES=OFF \
    -DOPENMP_STANDALONE_BUILD=ON
  "${cmake_bin}" --build "${build_dir}" -j"${jobs}"
  "${cmake_bin}" --install "${build_dir}"
}

if lightgbm_importable; then
  exit 0
fi

if brew_has_matching_bottle; then
  echo "Installing libomp from Homebrew bottle for LightGBM runtime..."
  HOMEBREW_NO_AUTO_UPDATE=1 brew install libomp
else
  echo "No matching Homebrew libomp bottle for macOS ${macos_major} ${arch}; using source build fallback."
  build_libomp_from_source
fi

if lightgbm_importable; then
  echo "LightGBM runtime is ready."
  exit 0
fi

echo "LightGBM still cannot load after libomp installation." >&2
exit 1
