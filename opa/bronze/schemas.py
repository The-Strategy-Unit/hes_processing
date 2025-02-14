"""OPA CSV schemas"""

import pyspark.sql.types as T

ALL_COLUMNS = {
    "1stoperation": T.StringType(),
    "activage": T.IntegerType(),
    "admincat": T.StringType(),
    "apptage": T.IntegerType(),
    "apptage_calc": T.DecimalType(10, 0),
    "apptdate": T.DateType(),
    "at_gp_practice": T.StringType(),
    "at_residence": T.StringType(),
    "at_treatment": T.StringType(),
    "atentype": T.StringType(),
    "attended": T.StringType(),
    "attendid": T.StringType(),
    "attendkey": T.StringType(),
    "attendkey_flag": T.StringType(),
    "babyage": T.StringType(),
    "cannet": T.StringType(),
    "canreg": T.StringType(),
    "carersi": T.StringType(),
    "ccg_gp_practice": T.StringType(),
    "ccg_residence": T.StringType(),
    "ccg_responsibility": T.StringType(),
    "ccg_responsibility_origin": T.StringType(),
    "ccg_treatment": T.StringType(),
    "ccg_treatment_origin": T.StringType(),
    "cr_gp_practice": T.StringType(),
    "cr_residence": T.StringType(),
    "cr_treatment": T.StringType(),
    "csnum": T.StringType(),
    "currward": T.StringType(),
    "currward_ons": T.StringType(),
    "diag3": T.StringType(),
    "diag4": T.StringType(),
    "diag_01": T.StringType(),
    "diag_02": T.StringType(),
    "diag_03": T.StringType(),
    "diag_04": T.StringType(),
    "diag_05": T.StringType(),
    "diag_06": T.StringType(),
    "diag_07": T.StringType(),
    "diag_08": T.StringType(),
    "diag_09": T.StringType(),
    "diag_10": T.StringType(),
    "diag_11": T.StringType(),
    "diag_12": T.StringType(),
    "diag_3_01": T.StringType(),
    "diag_3_02": T.StringType(),
    "diag_3_03": T.StringType(),
    "diag_3_04": T.StringType(),
    "diag_3_05": T.StringType(),
    "diag_3_06": T.StringType(),
    "diag_3_07": T.StringType(),
    "diag_3_08": T.StringType(),
    "diag_3_09": T.StringType(),
    "diag_3_10": T.StringType(),
    "diag_3_11": T.StringType(),
    "diag_3_12": T.StringType(),
    "diag_3_concat": T.StringType(),
    "diag_4_01": T.StringType(),
    "diag_4_02": T.StringType(),
    "diag_4_03": T.StringType(),
    "diag_4_04": T.StringType(),
    "diag_4_05": T.StringType(),
    "diag_4_06": T.StringType(),
    "diag_4_07": T.StringType(),
    "diag_4_08": T.StringType(),
    "diag_4_09": T.StringType(),
    "diag_4_10": T.StringType(),
    "diag_4_11": T.StringType(),
    "diag_4_12": T.StringType(),
    "diag_4_concat": T.StringType(),
    "diag_count": T.StringType(),
    "dnadate": T.DateType(),
    "dob_cfl": T.IntegerType(),
    "earldatoff": T.DateType(),
    "encrypted_hesid": T.StringType(),
    "ethnos": T.StringType(),
    "ethrawl": T.StringType(),
    "firstatt": T.StringType(),
    "fyear": T.StringType(),
    "gortreat": T.StringType(),
    "gpprac": T.StringType(),
    "gppracha": T.StringType(),
    "gppracro": T.StringType(),
    "gpprpct": T.StringType(),
    "gpprstha": T.StringType(),
    "hatreat": T.StringType(),
    "hrgnhs": T.StringType(),
    "hrgnhsvn": T.StringType(),
    "imd04": T.FloatType(),
    "imd04_decile": T.StringType(),
    "imd04c": T.FloatType(),
    "imd04ed": T.FloatType(),
    "imd04em": T.FloatType(),
    "imd04hd": T.FloatType(),
    "imd04hs": T.FloatType(),
    "imd04i": T.FloatType(),
    "imd04ia": T.FloatType(),
    "imd04ic": T.FloatType(),
    "imd04le": T.FloatType(),
    "imd04rk": T.FloatType(),
    "locclass": T.StringType(),
    "loctype": T.StringType(),
    "lsoa01": T.StringType(),
    "lsoa11": T.StringType(),
    "mainspef": T.StringType(),
    "marstat": T.StringType(),
    "msoa01": T.StringType(),
    "msoa11": T.StringType(),
    "mydob": T.StringType(),
    "newnhsno_check": T.StringType(),
    "nhsnoind": T.StringType(),
    "nodiags": T.StringType(),
    "noprocs": T.StringType(),
    "oacode6": T.StringType(),
    "opcs43": T.StringType(),
    "operstat": T.StringType(),
    "opertn3": T.StringType(),
    "opertn_01": T.StringType(),
    "opertn_02": T.StringType(),
    "opertn_03": T.StringType(),
    "opertn_04": T.StringType(),
    "opertn_05": T.StringType(),
    "opertn_06": T.StringType(),
    "opertn_07": T.StringType(),
    "opertn_08": T.StringType(),
    "opertn_09": T.StringType(),
    "opertn_10": T.StringType(),
    "opertn_11": T.StringType(),
    "opertn_12": T.StringType(),
    "opertn_13": T.StringType(),
    "opertn_14": T.StringType(),
    "opertn_15": T.StringType(),
    "opertn_16": T.StringType(),
    "opertn_17": T.StringType(),
    "opertn_18": T.StringType(),
    "opertn_19": T.StringType(),
    "opertn_20": T.StringType(),
    "opertn_21": T.StringType(),
    "opertn_22": T.StringType(),
    "opertn_23": T.StringType(),
    "opertn_24": T.StringType(),
    "opertn_3_01": T.StringType(),
    "opertn_3_02": T.StringType(),
    "opertn_3_03": T.StringType(),
    "opertn_3_04": T.StringType(),
    "opertn_3_05": T.StringType(),
    "opertn_3_06": T.StringType(),
    "opertn_3_07": T.StringType(),
    "opertn_3_08": T.StringType(),
    "opertn_3_09": T.StringType(),
    "opertn_3_10": T.StringType(),
    "opertn_3_11": T.StringType(),
    "opertn_3_12": T.StringType(),
    "opertn_3_13": T.StringType(),
    "opertn_3_14": T.StringType(),
    "opertn_3_15": T.StringType(),
    "opertn_3_16": T.StringType(),
    "opertn_3_17": T.StringType(),
    "opertn_3_18": T.StringType(),
    "opertn_3_19": T.StringType(),
    "opertn_3_20": T.StringType(),
    "opertn_3_21": T.StringType(),
    "opertn_3_22": T.StringType(),
    "opertn_3_23": T.StringType(),
    "opertn_3_24": T.StringType(),
    "opertn_4_01": T.StringType(),
    "opertn_4_02": T.StringType(),
    "opertn_4_03": T.StringType(),
    "opertn_4_04": T.StringType(),
    "opertn_4_05": T.StringType(),
    "opertn_4_06": T.StringType(),
    "opertn_4_07": T.StringType(),
    "opertn_4_08": T.StringType(),
    "opertn_4_09": T.StringType(),
    "opertn_4_10": T.StringType(),
    "opertn_4_11": T.StringType(),
    "opertn_4_12": T.StringType(),
    "opertn_4_13": T.StringType(),
    "opertn_4_14": T.StringType(),
    "opertn_4_15": T.StringType(),
    "opertn_4_16": T.StringType(),
    "opertn_4_17": T.StringType(),
    "opertn_4_18": T.StringType(),
    "opertn_4_19": T.StringType(),
    "opertn_4_20": T.StringType(),
    "opertn_4_21": T.StringType(),
    "opertn_4_22": T.StringType(),
    "opertn_4_23": T.StringType(),
    "opertn_4_24": T.StringType(),
    "opertn_4_concat": T.StringType(),
    "orgpppid": T.StringType(),
    "outcome": T.StringType(),
    "partyear": T.IntegerType(),
    "pcfound": T.StringType(),
    "pcon": T.StringType(),
    "pcon_ons": T.StringType(),
    "pconsult": T.StringType(),
    "pctcode": T.StringType(),
    "pctcode02": T.StringType(),
    "pctcode06": T.StringType(),
    "pctcode_his": T.StringType(),
    "pctnhs": T.StringType(),
    "pctorig": T.StringType(),
    "pctorig02": T.StringType(),
    "pctorig06": T.StringType(),
    "pctorig_his": T.StringType(),
    "pcttreat": T.StringType(),
    "perend": T.DateType(),
    "person_id_deid": T.StringType(),
    "perstart": T.DateType(),
    "pgpprac": T.StringType(),
    "postdist": T.StringType(),
    "preferer": T.StringType(),
    "preggmp": T.StringType(),
    "primerecp": T.StringType(),
    "priority": T.StringType(),
    "procode": T.StringType(),
    "procode3": T.StringType(),
    "procode5": T.StringType(),
    "procodet": T.StringType(),
    "protype": T.StringType(),
    "purcode": T.StringType(),
    "purstha": T.StringType(),
    "purval": T.StringType(),
    "referorg": T.StringType(),
    "refsourc": T.StringType(),
    "reqdate": T.DateType(),
    "rescty": T.StringType(),
    "rescty_ons": T.StringType(),
    "resgor": T.StringType(),
    "resgor_ons": T.StringType(),
    "resha": T.StringType(),
    "resladst": T.StringType(),
    "resladst_ons": T.StringType(),
    "respct": T.StringType(),
    "respct02": T.StringType(),
    "respct06": T.StringType(),
    "respct_his": T.StringType(),
    "resro": T.StringType(),
    "resstha": T.StringType(),
    "resstha02": T.StringType(),
    "resstha06": T.StringType(),
    "resstha_his": T.StringType(),
    "rotreat": T.StringType(),
    "rttperend": T.DateType(),
    "rttperstart": T.DateType(),
    "rttperstat": T.StringType(),
    "rururb_ind": T.StringType(),
    "sender": T.StringType(),
    "servtype": T.StringType(),
    "sex": T.StringType(),
    "sitetret": T.StringType(),
    "soal": T.StringType(),
    "soam": T.StringType(),
    "stafftyp": T.StringType(),
    "sthatret": T.StringType(),
    "subdate": T.DateType(),
    "sushrg": T.StringType(),
    "sushrgvers": T.StringType(),
    "suslddate": T.DateType(),
    "susrecid": T.LongType(),
    "susspellid": T.LongType(),
    "tretspef": T.StringType(),
    "wait_ind": T.StringType(),
    "waitdays": T.IntegerType(),
    "waiting": T.IntegerType(),
    "ward91": T.StringType(),
}


def create_schema(file: str, sep: str) -> T.StructType:
    """create csv schema"""
    with open(file, "r", encoding="UTF-8") as f:
        cols = f.read().lower().split(sep)

    return T.StructType([T.StructField(c, ALL_COLUMNS[c], True) for c in cols])
