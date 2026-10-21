"""Agrega vendas sintéticas usando centavos inteiros, sem arredondamento."""
def totals_by_customer(rows):
    totals = {}
    for customer, amount_cents in rows:
        if not isinstance(customer, str) or not customer.strip():
            raise ValueError("customer deve ser texto não vazio")
        if type(amount_cents) is not int or amount_cents < 0:
            raise ValueError("amount_cents deve ser inteiro não negativo")
        customer = customer.strip()
        totals[customer] = totals.get(customer, 0) + amount_cents
    return dict(sorted(totals.items()))

