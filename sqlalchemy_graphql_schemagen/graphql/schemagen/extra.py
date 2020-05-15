import graphene
from graphene.utils.subclass_with_meta import SubclassWithMeta_Meta


################################
# Order By Operations
################################
class OrderByOperation(graphene.Enum):
    ASC = "ASC"
    DESC = "DESC"


################################
# Filter Operations
################################
class FilterOperation(graphene.Enum):
    EQ = "EQ"
    NEQ = "NEQ"

    IS = "IS"
    ISNOT = "ISNOT"

    LT = "LT"
    GT = "GT"

    LIKE = "LIKE"
    NOTLIKE = "NOTLIKE"

    ILIKE = "ILIKE"
    NOTILIKE = "NOTILIKE"


################################
# <type>FilterOp GraphQL Object
################################

__SCHEMAGEN_filter_op_type_class_registry = {}


def create_or_get_graphql_filter_op_type_class(graphql_type: SubclassWithMeta_Meta):
    # Global repository of <typed>FilterOperations
    global __SCHEMAGEN_filter_op_type_class_registry

    # The GraphQL type name we are going to create a filter op type to.
    graphql_type_name = graphql_type.__name__

    if graphql_type_name not in __SCHEMAGEN_filter_op_type_class_registry:
        # Create
        fop_class_name = f"{graphql_type_name}FilterOp"
        fop = type(
            fop_class_name,
            (graphene.InputObjectType,),
            {
                "op": graphene.Field(FilterOperation, required=True),
                "v": graphene.Field(graphql_type, required=False, default_value=None),
            },
        )

        __SCHEMAGEN_filter_op_type_class_registry[graphql_type_name] = fop

    return __SCHEMAGEN_filter_op_type_class_registry[graphql_type_name]
