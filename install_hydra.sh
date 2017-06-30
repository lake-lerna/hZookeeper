#!/usr/bin/env bash
set -e
WorkDir=`pwd`
echo "==> Setting WorkDir=$WorkDir"
echo "==> Removing ${WorkDir}/venv_test"
rm -rf ${WorkDir}/venv_test
echo "==> Creating new virtual environment at ${WorkDir}/venv_test"
virtualenv ${WorkDir}/venv_test
source ${WorkDir}/venv_test/bin/activate
echo "==> Installing Hydra"
cd ${WorkDir}/hydra
pip install numpy
pip install pybuilder
pyb install_dependencies
pyb analyze
pyb publish -x run_unit_tests -x run_integration_tests -x verify
pyb install -x run_unit_tests -x run_integration_tests -x verify
cd $WorkDir
echo "Hydra is now installed inside the virtual enviourenment at ${WordDir}/venv_test"
echo "TO Activate the virtual enviourenment use >source ${WorkDir}/venv_test/bin/activate"
