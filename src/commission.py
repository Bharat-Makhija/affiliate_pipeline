def load_commission_config(path):
    import json
    with open(path) as f:
        return json.load(f)

def calculate_commission(network, amount, config):
    rules = config.get(network, config.get("default"))
    rate = rules['baseRate']
    if 'bonus' in rules and float(amount) >= rules['bonus']['minAmount']:
        rate += rules['bonus']['additionalRate']
    return round(float(amount) * rate, 2)
