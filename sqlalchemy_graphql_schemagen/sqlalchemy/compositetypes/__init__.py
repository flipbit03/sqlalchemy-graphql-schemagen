from decimal import Decimal


class Money(object):
    """Class to Represent a Monetary Value"""

    def __init__(self, currency: str, value: Decimal):
        self.currency = currency
        self.value = value

    def __composite_values__(self):
        return self.currency, self.value

    def __repr__(self):
        return "MoneyComposite(curr=%r, value=%r)" % (self.currency, self.value)

    def __eq__(self, other):
        return (
            isinstance(other, Money)
            and other.currency == self.currency
            and other.value == self.value
        )

    def __ne__(self, other):
        return not self.__eq__(other)

    def __str__(self):
        return f"{self.currency if self.currency else ''} {self.value if self.value else ''}".strip()
