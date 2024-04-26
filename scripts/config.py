"""aparelho de ginástica"""

import os

local_path = os.path.abspath(os.getcwd())
data_path = os.path.dirname(local_path) 


configs = {
    "meta_path": f"{local_path}/metadado.xlsx",
    "raw_path": f"{data_path}/data/raw/raw_",
    "work_path": f"{data_path}/data/work/work_cadastro.csv",
}