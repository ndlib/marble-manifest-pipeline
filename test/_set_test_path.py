import os
import sys

where_i_am = os.path.dirname(os.path.realpath(__file__))
sys.path.append(where_i_am + "/../")
sys.path.append(where_i_am + "./fixtures")
sys.path.append(where_i_am + "/../pipelineutilities/pipelineutilities/")
sys.path.append(where_i_am + "/../process_manifest/")
sys.path.append(where_i_am + "/../finalize/")
sys.path.append(where_i_am + "/../init/")
