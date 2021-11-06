#!/bin/zsh
if [[ `uname -m` == 'arm64' ]]; then
	if python -c 'import sys; assert sys.version_info >= (3, 9),"Python version must be greater than 3.9 on M1"'; then
           echo "Python version good"
        else
           echo "Python version must be at least 3.9 on M1 to proceeed"
           exit 1
        fi 
	brew install openblas
	#pip3 uninstall -y numpy pythran
	pip3 install cython pybind11
	if pip freeze | grep numpy
	then
  	echo "numpy installed, skipping"
	else
  	echo "numpy not installed"
  	pip3 install --no-binary :all: --no-use-pep517 numpy
fi
pip3 install pythran
export OPENBLAS=/opt/homebrew/opt/openblas/lib/
if pip freeze | grep scipy
then 
  pip3 install --no-binary :all: --no-use-pep517 scipy
fi
fi
