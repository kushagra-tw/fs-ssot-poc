set -euxo pipefail

export FILE_DATE_SUFFIX=$(date +%Y%m%d)

pip install -r requirements.txt

python domains/customer/school_mastering.py
python domains/customer/prettify_schools.py
python domains/customer/customer_mastering.py
python domains/customer/prettify_customers.py