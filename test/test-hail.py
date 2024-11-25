import hail as hl

"""
Test if Hail is installed correctly and can be imported in this conda environment.

Run this script with:
     python test-hail.py
"""

hl.init(local='local[4]',
        spark_conf={'spark.driver.memory': '8g'},
        default_reference="GRCh38")

mt = hl.balding_nichols_model(n_populations=3,
                              n_samples=10,
                              n_variants=100)
mt.show()

hl.stop()
