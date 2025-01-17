import re

product_name = "Samsung Galaxy S21 Fe White"
product_name_lower = product_name.lower()

def check_clean(product_name):
    colors = ["black", "silver", "gold", "gray", "grey", "blue", "green", "white", 
                "space gray", "space grey", "midnight", "mint", "minty", "lavender"]
    if any(color in product_name_lower for color in colors):
        # First handle compound colors with spaces
        product_name = re.sub(
            r'\s+space\s+gr[ae]y\s*',
            ' ',
            product_name,
            flags=re.IGNORECASE
        ).strip()
        # Then handle single-word colors
        product_name = re.sub(
            r'\s+(black|silver|gold|gr[ae]y|blue|green|white|midnight|mint|lavender)\s*',
            ' ',
            product_name,
            flags=re.IGNORECASE
        ).strip()
    return product_name

print(check_clean(product_name))