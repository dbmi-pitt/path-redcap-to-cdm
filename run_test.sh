export ORACLE_HOME=/opt/oracle/instantclient
export LD_LIBRARY_PATH=$ORACLE_HOME
export PYTHONPATH=$PYTHONPATH:.:./src/tests
python2.7 src/tests/edu/pitt/dbmi/redcap/redcap_test.py 2>&1 | tee redcap_test.log.$$