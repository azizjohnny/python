import re
from Dataclean import process_product_name

product_name = "Смартфон Oppo A18 4/128 Glowing Blue"
product_name_lower = product_name.lower()

print(process_product_name(product_name, "Smartfonlar"))