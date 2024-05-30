base_dir=`pwd`
base_lib=${base_dir}/bin
base_py=${base_dir}
echo base_dir=$base_dir
echo base_lib=$base_lib
echo base_py=$base_py

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$base_lib
echo LD_LIBRARY_PATH=$LD_LIBRARY_PATH
export PYTHONPATH=$PYTHONPATH:$base_lib:$base_py
echo PYTHONPATH=$PYTHONPATH