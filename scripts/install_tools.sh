GALAXY_INSTANCE=$1
API_KEY=$2
NO_DEPS=${3:-false}

INSTALL_DEPS="--install_resolver_dependencies"

if [ "$NO_DEPS" = "no-deps" ]; then
  INSTALL_DEPS="--skip_install_resolver_dependencies"
  echo "Not using --install_resolver_dependencies and using --skip_install_resolver_dependencies"
fi

for lock_file in $(ls $DIR_WITH_LOCK_FILES | grep .lock); do
  name=$( echo $lock_file | sed 's/.yaml.lock//' )
  shed-tools install $INSTALL_DEPS \
    --toolsfile $lock_file \
    --galaxy $GALAXY_INSTANCE \
    --api_key $API_KEY 2>&1 | tee -a $name.log
done