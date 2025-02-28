"""APC CSV schemas"""

import pyspark.sql.types as T

ALL_COLUMNS = {
    "acpdisp_1": T.StringType(),
    "acpdisp_2": T.StringType(),
    "acpdisp_3": T.StringType(),
    "acpdisp_4": T.StringType(),
    "acpdisp_5": T.StringType(),
    "acpdisp_6": T.StringType(),
    "acpdisp_7": T.StringType(),
    "acpdisp_8": T.StringType(),
    "acpdisp_9": T.StringType(),
    "acpdqind_1": T.StringType(),
    "acpdqind_2": T.StringType(),
    "acpdqind_3": T.StringType(),
    "acpdqind_4": T.StringType(),
    "acpdqind_5": T.StringType(),
    "acpdqind_6": T.StringType(),
    "acpdqind_7": T.StringType(),
    "acpdqind_8": T.StringType(),
    "acpdqind_9": T.StringType(),
    "acpend_1": T.DateType(),
    "acpend_2": T.DateType(),
    "acpend_3": T.DateType(),
    "acpend_4": T.DateType(),
    "acpend_5": T.DateType(),
    "acpend_6": T.DateType(),
    "acpend_7": T.DateType(),
    "acpend_8": T.DateType(),
    "acpend_9": T.DateType(),
    "acploc_1": T.StringType(),
    "acploc_2": T.StringType(),
    "acploc_3": T.StringType(),
    "acploc_4": T.StringType(),
    "acploc_5": T.StringType(),
    "acploc_6": T.StringType(),
    "acploc_7": T.StringType(),
    "acploc_8": T.StringType(),
    "acploc_9": T.StringType(),
    "acpn_1": T.IntegerType(),
    "acpn_2": T.IntegerType(),
    "acpn_3": T.IntegerType(),
    "acpn_4": T.IntegerType(),
    "acpn_5": T.IntegerType(),
    "acpn_6": T.IntegerType(),
    "acpn_7": T.IntegerType(),
    "acpn_8": T.IntegerType(),
    "acpn_9": T.IntegerType(),
    "acpout_1": T.StringType(),
    "acpout_2": T.StringType(),
    "acpout_3": T.StringType(),
    "acpout_4": T.StringType(),
    "acpout_5": T.StringType(),
    "acpout_6": T.StringType(),
    "acpout_7": T.StringType(),
    "acpout_8": T.StringType(),
    "acpout_9": T.StringType(),
    "acpplan_1": T.StringType(),
    "acpplan_2": T.StringType(),
    "acpplan_3": T.StringType(),
    "acpplan_4": T.StringType(),
    "acpplan_5": T.StringType(),
    "acpplan_6": T.StringType(),
    "acpplan_7": T.StringType(),
    "acpplan_8": T.StringType(),
    "acpplan_9": T.StringType(),
    "acpsour_1": T.StringType(),
    "acpsour_2": T.StringType(),
    "acpsour_3": T.StringType(),
    "acpsour_4": T.StringType(),
    "acpsour_5": T.StringType(),
    "acpsour_6": T.StringType(),
    "acpsour_7": T.StringType(),
    "acpsour_8": T.StringType(),
    "acpsour_9": T.StringType(),
    "acpspef_1": T.StringType(),
    "acpspef_2": T.StringType(),
    "acpspef_3": T.StringType(),
    "acpspef_4": T.StringType(),
    "acpspef_5": T.StringType(),
    "acpspef_6": T.StringType(),
    "acpspef_7": T.StringType(),
    "acpspef_8": T.StringType(),
    "acpspef_9": T.StringType(),
    "acpstar_1": T.DateType(),
    "acpstar_2": T.DateType(),
    "acpstar_3": T.DateType(),
    "acpstar_4": T.DateType(),
    "acpstar_5": T.DateType(),
    "acpstar_6": T.DateType(),
    "acpstar_7": T.DateType(),
    "acpstar_8": T.DateType(),
    "acpstar_9": T.DateType(),
    "acscflag": T.IntegerType(),
    "activage": T.IntegerType(),
    "adm_cfl": T.IntegerType(),
    "admiage": T.IntegerType(),
    "admidate": T.DateType(),
    "admimeth": T.StringType(),
    "admincat": T.StringType(),
    "admincatst": T.StringType(),
    "admisorc": T.StringType(),
    "admistat": T.StringType(),
    "aekey": T.StringType(),
    "alcdiag": T.StringType(),
    "alcdiag_4": T.StringType(),
    "alcfrac": T.FloatType(),
    "anagest": T.IntegerType(),
    "anasdate": T.DateType(),
    "antedur": T.IntegerType(),
    "at_gp_practice": T.StringType(),
    "at_residence": T.StringType(),
    "at_treatment": T.StringType(),
    "bedyear": T.IntegerType(),
    "biresus_1": T.StringType(),
    "biresus_2": T.StringType(),
    "biresus_3": T.StringType(),
    "biresus_4": T.StringType(),
    "biresus_5": T.StringType(),
    "biresus_6": T.StringType(),
    "biresus_7": T.StringType(),
    "biresus_8": T.StringType(),
    "biresus_9": T.StringType(),
    "birordr_1": T.StringType(),
    "birordr_2": T.StringType(),
    "birordr_3": T.StringType(),
    "birordr_4": T.StringType(),
    "birordr_5": T.StringType(),
    "birordr_6": T.StringType(),
    "birordr_7": T.StringType(),
    "birordr_8": T.StringType(),
    "birordr_9": T.StringType(),
    "birstat_1": T.StringType(),
    "birstat_2": T.StringType(),
    "birstat_3": T.StringType(),
    "birstat_4": T.StringType(),
    "birstat_5": T.StringType(),
    "birstat_6": T.StringType(),
    "birstat_7": T.StringType(),
    "birstat_8": T.StringType(),
    "birstat_9": T.StringType(),
    "birweit_1": T.IntegerType(),
    "birweit_2": T.IntegerType(),
    "birweit_3": T.IntegerType(),
    "birweit_4": T.IntegerType(),
    "birweit_5": T.IntegerType(),
    "birweit_6": T.IntegerType(),
    "birweit_7": T.IntegerType(),
    "birweit_8": T.IntegerType(),
    "birweit_9": T.IntegerType(),
    "cannet": T.StringType(),
    "canreg": T.StringType(),
    "carersi": T.StringType(),
    "category": T.StringType(),
    "cause": T.StringType(),
    "cause3": T.StringType(),
    "cause4": T.StringType(),
    "cause_3": T.StringType(),
    "cause_4": T.StringType(),
    "ccg_gp_practice": T.StringType(),
    "ccg_residence": T.StringType(),
    "ccg_responsibility": T.StringType(),
    "ccg_responsibility_origin": T.StringType(),
    "ccg_treatment": T.StringType(),
    "ccg_treatment_origin": T.StringType(),
    "cdsextdate": T.DateType(),
    "cdsverprotid": T.StringType(),
    "cdsversion": T.StringType(),
    "cendur": T.IntegerType(),
    "censage": T.IntegerType(),
    "censtat": T.StringType(),
    "cenward": T.StringType(),
    "classpat": T.StringType(),
    "cr_gp_practice": T.StringType(),
    "cr_residence": T.StringType(),
    "cr_treatment": T.StringType(),
    "csnum": T.StringType(),
    "currward": T.StringType(),
    "currward_ons": T.StringType(),
    "delchang": T.StringType(),
    "delinten": T.StringType(),
    "delmeth_1": T.StringType(),
    "delmeth_2": T.StringType(),
    "delmeth_3": T.StringType(),
    "delmeth_4": T.StringType(),
    "delmeth_5": T.StringType(),
    "delmeth_6": T.StringType(),
    "delmeth_7": T.StringType(),
    "delmeth_8": T.StringType(),
    "delmeth_9": T.StringType(),
    "delmeth_d": T.StringType(),
    "delonset": T.IntegerType(),
    "delplac_1": T.StringType(),
    "delplac_2": T.StringType(),
    "delplac_3": T.StringType(),
    "delplac_4": T.StringType(),
    "delplac_5": T.StringType(),
    "delplac_6": T.StringType(),
    "delplac_7": T.StringType(),
    "delplac_8": T.StringType(),
    "delplac_9": T.StringType(),
    "delposan": T.StringType(),
    "delprean": T.StringType(),
    "delstat_1": T.StringType(),
    "delstat_2": T.StringType(),
    "delstat_3": T.StringType(),
    "delstat_4": T.StringType(),
    "delstat_5": T.StringType(),
    "delstat_6": T.StringType(),
    "delstat_7": T.StringType(),
    "delstat_8": T.StringType(),
    "delstat_9": T.StringType(),
    "depdays_1": T.IntegerType(),
    "depdays_2": T.IntegerType(),
    "depdays_3": T.IntegerType(),
    "depdays_4": T.IntegerType(),
    "depdays_5": T.IntegerType(),
    "depdays_6": T.IntegerType(),
    "depdays_7": T.IntegerType(),
    "depdays_8": T.IntegerType(),
    "depdays_9": T.IntegerType(),
    "det_cfl": T.IntegerType(),
    "detdur": T.IntegerType(),
    "detndate": T.DateType(),
    "diag3_01": T.StringType(),
    "diag4_01": T.StringType(),
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
    "diag_13": T.StringType(),
    "diag_14": T.StringType(),
    "diag_15": T.StringType(),
    "diag_16": T.StringType(),
    "diag_17": T.StringType(),
    "diag_18": T.StringType(),
    "diag_19": T.StringType(),
    "diag_20": T.StringType(),
    "diag_count": T.IntegerType(),
    "dis_cfl": T.IntegerType(),
    "disdate": T.DateType(),
    "disdest": T.StringType(),
    "dismeth": T.StringType(),
    "disreadydate": T.DateType(),
    "dob_cfl": T.IntegerType(),
    "domproc": T.StringType(),
    "earldatoff": T.DateType(),
    "elec_cfl": T.IntegerType(),
    "elecdate": T.DateType(),
    "elecdur": T.IntegerType(),
    "elecdur_calc": T.IntegerType(),
    "encrypted_hesid": T.StringType(),
    "endage": T.IntegerType(),
    "epidur": T.IntegerType(),
    "epie_cfl": T.IntegerType(),
    "epiend": T.DateType(),
    "epikey": T.StringType(),
    "epiorder": T.IntegerType(),
    "epis_cfl": T.IntegerType(),
    "epistart": T.DateType(),
    "epistat": T.IntegerType(),
    "epitype": T.IntegerType(),
    "ethnos": T.StringType(),
    "ethraw": T.StringType(),
    "fae": T.IntegerType(),
    "fae_emergency": T.IntegerType(),
    "fce": T.IntegerType(),
    "fde": T.IntegerType(),
    "firstreg": T.StringType(),
    "fyear": T.StringType(),
    "gestat_1": T.IntegerType(),
    "gestat_2": T.IntegerType(),
    "gestat_3": T.IntegerType(),
    "gestat_4": T.IntegerType(),
    "gestat_5": T.IntegerType(),
    "gestat_6": T.IntegerType(),
    "gestat_7": T.IntegerType(),
    "gestat_8": T.IntegerType(),
    "gestat_9": T.IntegerType(),
    "gortreat": T.StringType(),
    "gpprac": T.StringType(),
    "gppracha": T.StringType(),
    "gppracro": T.StringType(),
    "gpprpct": T.StringType(),
    "gpprstha": T.StringType(),
    "hatreat": T.StringType(),
    "hneoind": T.StringType(),
    "hrglate35": T.StringType(),
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
    "intdays_1": T.IntegerType(),
    "intdays_2": T.IntegerType(),
    "intdays_3": T.IntegerType(),
    "intdays_4": T.IntegerType(),
    "intdays_5": T.IntegerType(),
    "intdays_6": T.IntegerType(),
    "intdays_7": T.IntegerType(),
    "intdays_8": T.IntegerType(),
    "intdays_9": T.IntegerType(),
    "intmanig": T.StringType(),
    "lsoa01": T.StringType(),
    "lsoa11": T.StringType(),
    "mainspef": T.StringType(),
    "marstat": T.StringType(),
    "matage": T.IntegerType(),
    "maternity_episode_type": T.StringType(),
    "mentcat": T.StringType(),
    "msoa01": T.StringType(),
    "msoa11": T.StringType(),
    "mydob": T.StringType(),
    "neocare": T.StringType(),
    "neodur": T.IntegerType(),
    "newnhsno_check": T.StringType(),
    "nhsnoind": T.StringType(),
    "numacp": T.IntegerType(),
    "numbaby": T.StringType(),
    "numpreg": T.IntegerType(),
    "numtailb": T.IntegerType(),
    "oacode6": T.StringType(),
    "opcs43": T.StringType(),
    "opdate_01": T.DateType(),
    "opdate_02": T.DateType(),
    "opdate_03": T.DateType(),
    "opdate_04": T.DateType(),
    "opdate_05": T.DateType(),
    "opdate_06": T.DateType(),
    "opdate_07": T.DateType(),
    "opdate_08": T.DateType(),
    "opdate_09": T.DateType(),
    "opdate_10": T.DateType(),
    "opdate_11": T.DateType(),
    "opdate_12": T.DateType(),
    "opdate_13": T.DateType(),
    "opdate_14": T.DateType(),
    "opdate_15": T.DateType(),
    "opdate_16": T.DateType(),
    "opdate_17": T.DateType(),
    "opdate_18": T.DateType(),
    "opdate_19": T.DateType(),
    "opdate_20": T.DateType(),
    "opdate_21": T.DateType(),
    "opdate_22": T.DateType(),
    "opdate_23": T.DateType(),
    "opdate_24": T.DateType(),
    "operstat": T.StringType(),
    "opertn3_01": T.StringType(),
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
    "opertn_count": T.IntegerType(),
    "orgpppid": T.StringType(),
    "orgsup_1": T.IntegerType(),
    "orgsup_2": T.IntegerType(),
    "orgsup_3": T.IntegerType(),
    "orgsup_4": T.IntegerType(),
    "orgsup_5": T.IntegerType(),
    "orgsup_6": T.IntegerType(),
    "orgsup_7": T.IntegerType(),
    "orgsup_8": T.IntegerType(),
    "orgsup_9": T.IntegerType(),
    "partyear": T.IntegerType(),
    "pcfound": T.StringType(),
    "pcgcode": T.StringType(),
    "pcgorig": T.StringType(),
    "pcon": T.StringType(),
    "pcon_ons": T.StringType(),
    "pconsult": T.StringType(),
    "pctcode": T.StringType(),
    "pctcode02": T.StringType(),
    "pctcode06": T.StringType(),
    "pctnhs": T.StringType(),
    "pctorig": T.StringType(),
    "pctorig02": T.StringType(),
    "pctorig06": T.StringType(),
    "pcttreat": T.StringType(),
    "person_id_deid": T.StringType(),
    "posopdur": T.IntegerType(),
    "postdist": T.StringType(),
    "postdur": T.IntegerType(),
    "preferer": T.StringType(),
    "preggmp": T.StringType(),
    "preopdur": T.IntegerType(),
    "primercp": T.StringType(),
    "procode": T.StringType(),
    "procode3": T.StringType(),
    "procode5": T.StringType(),
    "procodet": T.StringType(),
    "protype": T.StringType(),
    "provspno": T.LongType(),
    "provspnops": T.StringType(),
    "purcode": T.StringType(),
    "purro": T.StringType(),
    "purstha": T.StringType(),
    "purval": T.StringType(),
    "referorg": T.StringType(),
    "rescty": T.StringType(),
    "rescty_ons": T.StringType(),
    "resgor": T.StringType(),
    "resgor_ons": T.StringType(),
    "resha": T.StringType(),
    "resladst": T.StringType(),
    "resladst_currward": T.StringType(),
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
    "sex": T.StringType(),
    "sexbaby_1": T.StringType(),
    "sexbaby_2": T.StringType(),
    "sexbaby_3": T.StringType(),
    "sexbaby_4": T.StringType(),
    "sexbaby_5": T.StringType(),
    "sexbaby_6": T.StringType(),
    "sexbaby_7": T.StringType(),
    "sexbaby_8": T.StringType(),
    "sexbaby_9": T.StringType(),
    "sitetret": T.StringType(),
    "spelbgin": T.IntegerType(),
    "speldur": T.IntegerType(),
    "spelend": T.StringType(),
    "startage": T.IntegerType(),
    "startage_calc": T.FloatType(),
    "sthatret": T.StringType(),
    "subdate": T.DateType(),
    "suscorehrg": T.StringType(),
    "sushrg": T.StringType(),
    "sushrgvers": T.StringType(),
    "suslddate": T.DateType(),
    "susrecid": T.StringType(),
    "susspellid": T.StringType(),
    "tretspef": T.StringType(),
    "vind": T.StringType(),
    "waitdays": T.IntegerType(),
    "waitlist": T.IntegerType(),
    "ward91": T.StringType(),
    "wardstrt": T.StringType(),
    "well_baby_ind": T.StringType(),
}


def create_schema(file: str, sep: str) -> T.StructType:
    """create csv schema"""
    with open(file, "r", encoding="UTF-8") as f:
        cols = f.read().lower().split(sep)

    for i, c in enumerate(cols):
        match c:
            case "soal":
                cols[i] = "lsoa01"
            case "soam":
                cols[i] = "msoa01"
            case "my_dob":
                cols[i] = "mydob"
            case "pctorgig02":
                cols[i] = "pctorig02"
            case "pctorgig06":
                cols[i] = "pctorig06"
            case "respect06":
                cols[i] = "respct06"

    return T.StructType([T.StructField(c, ALL_COLUMNS[c], True) for c in cols])
