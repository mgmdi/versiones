from pybuilder.core import use_plugin, init

use_plugin("python.core")
use_plugin("python.unittest")
use_plugin("python.install_dependencies")


name = "Versiones"
default_task = "publish"


@init
def set_properties(project):
    pass
