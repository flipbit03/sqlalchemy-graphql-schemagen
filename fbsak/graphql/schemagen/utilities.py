from typing import List, Tuple, Dict, Callable

import graphene
from graphene.utils.str_converters import to_camel_case
from graphene.utils.subclass_with_meta import SubclassWithMeta_Meta
from graphene_sqlalchemy import SQLAlchemyObjectType
from graphene_sqlalchemy.converter import convert_sqlalchemy_type
from graphene_sqlalchemy.registry import get_global_registry
import sqlalchemy
from sqlalchemy import Column, inspect, ColumnDefault
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import Mapper, Session, Query
from sqlalchemy.orm.state import InstanceState

from . import HookDictType
from .extra import (
    FilterOperation,
    create_or_get_graphql_filter_op_type_class,
    OrderByOperation,
)


################################
################################
# Global Vars and Registries
################################
################################

# this variable holds a list of all graphene.Enums created
# with the table field names.
#
# we need to cache this and reuse accordingly since you cannot
# have repeated schema names in a GraphQL Schema definition.
from .hooks import HookOperation

__SCHEMAGEN_field_enum_by_sa_type_registry = {}


################################
# Custom SQLAlchemyObjectType Class - so we can define shared properties.
################################
class OurBaseSQLAlchemyObjectType(SQLAlchemyObjectType):
    class Meta:
        abstract = True


################################
################################
# SchemaGen's GraphQL Class Builder Helpers
################################
################################


################################
# Get all SQLAlchemy's Model Classes as a List
################################
def get_all_sa_model_classes(
    sa_model_base_class: DeclarativeMeta,
) -> List[DeclarativeMeta]:
    # Get SQLAlchemy Class Registry
    sa_class_registry: dict = getattr(sa_model_base_class, "_decl_class_registry")

    # List of all Model Classes from SQLAlchemy
    model_classes = [x for x in sa_class_registry.values() if hasattr(x, "__table__")]

    return model_classes


##################################################
# SQLAlchemySchemaGenerator Helper Functions
##################################################
def gql_query_build_sa_obj_type(sa_model_class: DeclarativeMeta):
    meta_model_class = type(
        "Meta", (), {"model": sa_model_class, "description": sa_model_class.__doc__}
    )
    sa_obj_type_class = type(
        f"{sa_model_class.__name__}",
        (OurBaseSQLAlchemyObjectType,),
        {"Meta": meta_model_class},
    )
    return sa_obj_type_class


################################
# SQLAlchemy Operations for order_by
################################
order_by_ops = {"ASC": sqlalchemy.asc, "DESC": sqlalchemy.desc}


################################
# Create the resolve_MODELs function for "get_all"
# - with filter
# - with pagination
################################
def make_resolve_func_maker(
    sa_model_class: DeclarativeMeta, get_session_func: Callable, hooks: HookDictType
) -> Callable:
    def resolve_func(_parent, _info, **kwargs):
        nonlocal sa_model_class

        # Get Mapper for SQLAlchemy's Model Class
        m: Mapper = inspect(sa_model_class)

        # Get the Session() so we can access the database
        s: Session = get_session_func()

        ################################
        # Base Query
        ################################
        q = s.query(sa_model_class)

        ################################
        # Filter
        ################################

        # List of Filters
        filter_obj_list = kwargs.get("filters", {})

        for filter_obj in filter_obj_list:
            for filter_name in filter_obj:
                filter_obj = filter_obj[filter_name]

                sa_column: Column = m.columns[filter_name]

                if FilterOperation.get(filter_obj.op) == FilterOperation.EQ:
                    q = q.filter(sa_column == filter_obj.v)

                elif FilterOperation.get(filter_obj.op) == FilterOperation.NEQ:
                    q = q.filter(sa_column != filter_obj.v)

                elif FilterOperation.get(filter_obj.op) == FilterOperation.IS:
                    q = q.filter(sa_column.is_(filter_obj.v))

                elif FilterOperation.get(filter_obj.op) == FilterOperation.ISNOT:
                    q = q.filter(sa_column.isnot(filter_obj.v))

                elif FilterOperation.get(filter_obj.op) == FilterOperation.LT:
                    q = q.filter(sa_column < filter_obj.v)

                elif FilterOperation.get(filter_obj.op) == FilterOperation.GT:
                    q = q.filter(sa_column > filter_obj.v)

                elif FilterOperation.get(filter_obj.op) == FilterOperation.LIKE:
                    q = q.filter(sa_column.like(f"%{filter_obj.v}%"))

                elif FilterOperation.get(filter_obj.op) == FilterOperation.NOTLIKE:
                    q = q.filter(sa_column.notlike(f"%{filter_obj.v}%"))

                elif FilterOperation.get(filter_obj.op) == FilterOperation.ILIKE:
                    q = q.filter(sa_column.ilike(f"%{filter_obj.v}%"))

                elif FilterOperation.get(filter_obj.op) == FilterOperation.NOTILIKE:
                    q = q.filter(sa_column.notilike(f"%{filter_obj.v}%"))

        ################################
        # ORDER_BY
        ################################

        order_by_params = kwargs.get("order_by")

        if order_by_params:
            order_by_function = order_by_ops[order_by_params.o]
            order_column = m.columns[order_by_params.f]

            q = q.order_by(order_by_function(order_column))

        ################################
        # LIMIT / OFFSET (Pagination)
        ################################

        # Pagination
        page = max(0, kwargs.get("page") - 1)
        perpage = kwargs.get("perpage")

        # Apply Pagination
        q = q.limit(perpage).offset(page * perpage)

        return q.all()

    # Bind final_resolve_func to the vanilla (without hooks) resolve function.
    final_resolve_func = resolve_func

    # Decorate resolve_func with the provided hooks class, if we have one.
    read_hook = hooks.get(HookOperation.READ)
    if read_hook:
        final_resolve_func = read_hook(resolve_func)

    # Generate Resolver Function Name
    # Example: "resolve_Users"
    mci: Mapper = inspect(sa_model_class)
    table_name = str(mci.tables[0].name)

    final_resolve_func_name = f"resolve_{table_name}"
    final_resolve_func.__name__ = final_resolve_func_name

    return final_resolve_func


################################
# Generate Documentation for each SQLAlchemy Model that is going to be mapped to a GraphQL Type
################################
def make_gql_object_description_from_sa_model_class(mc: DeclarativeMeta) -> str:
    mci: Mapper = inspect(mc)

    # Generate attributes to apply inside the docs.
    class_docstring = f'"{mc.__doc__}"\\\n' if mc.__doc__ else ""
    primary_key_name: str = mci.primary_key[0].key
    return f"""{class_docstring}pk: \"{primary_key_name}\""""


################################
# Convert any SQLAlchemy Model Instance (Query Result) to a plain dict()
################################
def sa_instance_to_dict(mc: DeclarativeMeta):
    """Transform a SQLAlchemy instance to a sanitized dict, without instance/control vars"""
    mci: InstanceState = inspect(mc)

    column_list = get_columns_from_sa_model_class(mci.class_)

    # get obj's columns from <Mapper>.columns and do a direct getattr() using those names
    return {c.name: getattr(mc, c.name) for c in column_list}


################################
# Grab a List of SQLAlchemy Columns from a SQLAlchemy Model
################################
def get_columns_from_sa_model_class(sa_model_class: DeclarativeMeta) -> List[Column]:
    # Model Classes return a inspection-able SQLAlchemy "Mapper" object.
    mci: Mapper = inspect(sa_model_class)

    return [column for column in mci.columns]


################################
# Grab GraphQL Field from a SQLAlchemy Column
################################

mask_type = Dict[SubclassWithMeta_Meta, SubclassWithMeta_Meta]

# by default, remap ID to "Int" for filter operations
mask_ID_to_Int: mask_type = {graphene.ID: graphene.Int}


def get_graphql_field_type_for_sa_column(
    column: Column, mask: mask_type = None
) -> SubclassWithMeta_Meta:
    original_graphql_type = convert_sqlalchemy_type(
        column.type, column, get_global_registry()
    )

    # Mask type using the mask argument.
    if mask:
        returned_graphql_type = mask.get(original_graphql_type, original_graphql_type)
        return returned_graphql_type

    # Normal return
    return original_graphql_type


################################
################################
# READ / QUERY
################################
################################


def create_gql_get_query_filter_input_object_type_from_sa_model(mc: DeclarativeMeta):
    sa_column_list = get_columns_from_sa_model_class(mc)

    class_items = {}
    for sa_column in sa_column_list:
        # For filter definitions, use 'Int' as type for 'Id'
        graphql_column_type = get_graphql_field_type_for_sa_column(
            sa_column, mask=mask_ID_to_Int
        )

        fop_class = create_or_get_graphql_filter_op_type_class(graphql_column_type)

        class_items[sa_column.name] = graphene.Field(fop_class)

    query_filter_params_input_object_type = type(
        f"{mc.__name__}QueryParams", (graphene.InputObjectType,), class_items
    )

    # Remember to wrap this around an graphene.Argument so it can be used in an actual field parameter
    return graphene.Argument(graphene.List(query_filter_params_input_object_type))


################################
# Create Graphene.Enum with the Column Names from a SQLAlchemy Model Class
################################
def create_or_get_gql_field_list_enum_from_sa_model(mc: DeclarativeMeta):
    global __SCHEMAGEN_field_enum_by_sa_type_registry
    field_enum_registry = __SCHEMAGEN_field_enum_by_sa_type_registry

    mc_name = mc.__name__
    if mc_name in field_enum_registry:
        return field_enum_registry[mc_name]

    # Todo: Respect camelCase setting from graphene. for now this is hardcoded to convert to camelcase
    def gen_enum_entry(field_name):
        return to_camel_case(field_name), field_name

    sa_column_list = get_columns_from_sa_model_class(mc)
    return graphene.Enum(
        f"{mc.__name__}FieldEnum", [gen_enum_entry(_c.name) for _c in sa_column_list]
    )


################################
# Query ORDER_BY
################################


def create_gql_get_query_order_by_filter_input_object_type_from_sa_model(
    mc: DeclarativeMeta,
):
    field_enum = create_or_get_gql_field_list_enum_from_sa_model(mc)

    class_items = {
        "f": graphene.Argument(field_enum, required=True),
        "o": graphene.Argument(OrderByOperation, required=True),
    }

    query_filter_params_input_object_type = type(
        f"{mc.__name__}OrderByParams", (graphene.InputObjectType,), class_items
    )

    # Remember to wrap this around an graphene.Argument so it can be used in an actual field parameter
    return graphene.Argument(query_filter_params_input_object_type)


################################
################################
# UPDATE
################################
################################

################################
# Object that validates the input arguments of the Update<Model> function
################################
def create_gql_update_input_object_type_from_sa_class(
    sa_model_class: DeclarativeMeta,
) -> type:
    sa_column_list = get_columns_from_sa_model_class(sa_model_class)

    input_class_fields = {}

    for sa_column in sa_column_list:
        graphql_type = get_graphql_field_type_for_sa_column(sa_column)

        field_args = {}
        # is this the Primary_key?
        if graphql_type == graphene.ID:
            field_args["required"] = True

        # Field doc= argument
        sa_column_doc_string = getattr(sa_column, "doc", None)
        if sa_column_doc_string:
            field_args["description"] = sa_column_doc_string

        # Populate Class Dict
        input_class_fields[sa_column.name] = graphql_type(**field_args)

    input_class_name = f"{sa_model_class.__name__}UpdateInput"
    input_class = type(
        input_class_name, (graphene.InputObjectType,), input_class_fields
    )

    return input_class


################################
# "Update" Mutation's Argument Class
# use the method above to get the proper input object type for this
################################
def create_gql_mutation_update_arguments_class(sa_model_class: DeclarativeMeta) -> type:
    obj_input_class = create_gql_update_input_object_type_from_sa_class(sa_model_class)

    # This is where we'll populate the soon-to-be updated class' arguments
    argument_name = f"{sa_model_class.__name__.lower()}_data"

    arg_class_items = {argument_name: obj_input_class(required=True)}

    arguments_class = type("Arguments", (), arg_class_items)

    return arguments_class


################################
# update<Model> MAIN FUNCTION
################################
def create_update_obj_mutation_object(
    sa_model_class: DeclarativeMeta, get_session_hook, hooks: HookDictType
) -> type:
    cls_name: str = sa_model_class.__name__

    # use the same name as the SQLAlchemy's class name
    new_graphql_obj_name = cls_name

    # get the graphql object associated with this SQLAlchemy Model Class
    gql_object = get_global_registry().get_type_for_model(sa_model_class)

    ################################
    # create a partial class to be able to use it inside the mutate() function ;-)
    ################################

    update_obj_partial_class_items = {
        # Update Mutation Arguments
        "Arguments": create_gql_mutation_update_arguments_class(sa_model_class),
        # The return value of the update mutation query (the same object)
        new_graphql_obj_name: graphene.Field(lambda: gql_object),
        # this needs to exist or else the class cannot be created, it needs this item to exist.
        "mutate": lambda: None,
    }

    # Create Partial UpdateObj Class
    update_obj_partial_class = type(
        f"PartialUpdate{cls_name}", (graphene.Mutation,), update_obj_partial_class_items
    )

    # Mutate Function Entry Point
    def mutate_func(root, info, **kwargs):
        # Get class name from the outside and build the parameter name dynamically
        param_name = f"{cls_name.lower()}_data"

        # Get the new instance data from kwargs[param_name]
        update_data: dict = kwargs.get(param_name)

        ###################
        # UPDATE DATA
        ###################

        # Get DB Session
        s: Session = get_session_hook()

        # primary key name
        pk_name = inspect(sa_model_class).primary_key[0].name

        # Get the Original Object by PRIMARY KEY ID
        def get_sa_class_by_pk_id(sa_class: DeclarativeMeta, pk_id: int) -> Query:
            x = s.query(sa_class).filter(getattr(sa_class, pk_name) == int(pk_id))

            return x

        # #### SQLAlchemy TIME ####

        # 1- get original entity from SA
        sa_obj = get_sa_class_by_pk_id(sa_model_class, update_data[pk_name]).one()

        # # 2- modify it
        for k, v in update_data.items():
            setattr(sa_obj, k, v)
        #
        # # 3- add to session
        s.add(sa_obj)

        # 4- commit
        s.commit()

        # 5- convert updated object to a dict
        sa_instance_as_dict = sa_instance_to_dict(sa_obj)
        partial_update_obj_class_invocation = {
            new_graphql_obj_name: gql_object(**sa_instance_as_dict)
        }

        return update_obj_partial_class(**partial_update_obj_class_invocation)

    # Decorate resolve_func with the provided hooks class, if we have one.
    final_mutate_func = mutate_func
    update_hook = hooks.get(HookOperation.UPDATE)
    if update_hook:
        final_mutate_func = update_hook(mutate_func)

    # Create Definitive Class
    definitive_update_obj_class_items = {"mutate": final_mutate_func}

    definitive_update_obj_class = type(
        f"Update{cls_name}",
        (update_obj_partial_class,),
        definitive_update_obj_class_items,
    )

    return definitive_update_obj_class


################################
################################
# CREATE
################################
################################

################################
# Object that validates the input arguments of the Create<Model> function
################################
def create_gql_create_input_object_type_from_sa_class(
    sa_model_class: DeclarativeMeta,
) -> type:
    sa_column_list = get_columns_from_sa_model_class(sa_model_class)

    input_class_fields = {}

    for sa_column in sa_column_list:
        graphql_type = get_graphql_field_type_for_sa_column(sa_column)

        # Skip the primary key: we are attempting to CREATE an Object
        # (which will receive a new id from the backend)
        if sa_column.primary_key:
            continue

        field_args = {}

        # Is this column NOT NULL? make it required
        if not sa_column.nullable:
            field_args["required"] = True

        # do we have a default value on this column?
        if sa_column.default:
            cd: ColumnDefault = sa_column.default

            # Disable 'required' above
            field_args["required"] = False

            # is the default an scalar
            if cd.is_scalar:
                # set the default value from that scalar.
                field_args["default_value"] = cd.arg

        # Field doc= argument
        sa_column_doc_string = getattr(sa_column, "doc", None)
        if sa_column_doc_string:
            field_args["description"] = sa_column_doc_string

        # Populate Class Dict
        input_class_fields[sa_column.name] = graphql_type(**field_args)

    input_class_name = f"{sa_model_class.__name__}CreateInput"
    input_class = type(
        input_class_name, (graphene.InputObjectType,), input_class_fields
    )

    return input_class


################################
# "Create" Mutation's Argument Class
################################
def create_gql_mutation_create_arguments_class(sa_model_class: DeclarativeMeta) -> type:
    obj_input_class = create_gql_create_input_object_type_from_sa_class(sa_model_class)

    # This is where we'll populate the soon-to-be updated class' arguments
    argument_name = f"{sa_model_class.__name__.lower()}_data"

    arg_class_items = {argument_name: obj_input_class(required=True)}

    arguments_class = type("Arguments", (), arg_class_items)

    return arguments_class


################################
# create<Model> MAIN FUNCTION
################################
def create_create_obj_mutation_object(
    sa_model_class: DeclarativeMeta, get_session_hook, hooks: HookDictType
) -> type:
    cls_name: str = sa_model_class.__name__

    # use the same name as the SQLAlchemy's class name
    new_graphql_obj_name = cls_name

    # get the graphql object associated with this SQLAlchemy Model Class
    gql_object = get_global_registry().get_type_for_model(sa_model_class)

    ################################
    # create a partial class to be able to use it inside the mutate() function ;-)
    ################################

    create_obj_partial_class_items = {
        # Update Mutation Arguments
        "Arguments": create_gql_mutation_create_arguments_class(sa_model_class),
        # The return value of the update mutation query (the same object)
        new_graphql_obj_name: graphene.Field(lambda: gql_object),
        # this needs to exist or else the class cannot be created, it needs this item to exist.
        "mutate": lambda: None,
    }

    # Create Partial CreateObj Class
    create_obj_partial_class = type(
        f"PartialCreate{cls_name}", (graphene.Mutation,), create_obj_partial_class_items
    )

    # Mutate Function Entry Point
    def mutate_func(root, info, **kwargs):
        # Get class name from the outside and build the parameter name dynamically
        param_name = f"{cls_name.lower()}_data"

        # Get DB Session
        s: Session = get_session_hook()

        # Get the new instance data from kwargs[param_name]
        create_data: dict = kwargs.get(param_name)

        ###################
        # CREATE DATA
        ###################

        # 1- Create a new object
        new_obj = sa_model_class()

        # 2- Add data from the GraphQL Request to the SQLAlchemy Object
        for k, v in create_data.items():
            setattr(new_obj, k, v)

        # 3- Add new obj to Session
        s.add(new_obj)

        # 4- Commit!
        s.commit()

        # 5- Convert newly created object to a dict and return it back
        sa_instance_as_dict = sa_instance_to_dict(new_obj)
        partial_create_obj_class_invocation = {
            new_graphql_obj_name: gql_object(**sa_instance_as_dict)
        }

        return create_obj_partial_class(**partial_create_obj_class_invocation)

    definitive_create_function = mutate_func
    create_hook = hooks.get(HookOperation.CREATE)
    if create_hook:
        definitive_create_function = create_hook(mutate_func)

    # Create Definitive Class
    definitive_create_obj_class_items = {"mutate": definitive_create_function}

    definitive_create_obj_class = type(
        f"Create{cls_name}",
        (create_obj_partial_class,),
        definitive_create_obj_class_items,
    )

    return definitive_create_obj_class


################################
################################
# DELETE
################################
################################

################################
# "Delete" Mutation's Argument Class
################################
def create_gql_mutation_delete_arguments_class(sa_model_class: DeclarativeMeta) -> type:

    pk_name = inspect(sa_model_class).primary_key[0].name

    arg_class_items = {pk_name: graphene.Int(required=True)}

    arguments_class = type("Arguments", (), arg_class_items)

    return arguments_class


################################
# delete<Model> MAIN FUNCTION
################################
def create_delete_obj_mutation_object(
    sa_model_class: DeclarativeMeta, get_session_hook, hooks: HookDictType
) -> type:
    cls_name: str = sa_model_class.__name__

    # use the same name as the SQLAlchemy's class name
    new_graphql_obj_name = cls_name

    # get the graphql object associated with this SQLAlchemy Model Class
    gql_object = get_global_registry().get_type_for_model(sa_model_class)

    ################################
    # create a partial class to be able to use it inside the mutate() function ;-)
    ################################

    # Argument name - the count of deleted items (1 or 0)
    deleted_count_arg_name = "deleted_count"

    delete_obj_partial_class_items = {
        # Update Mutation Arguments
        "Arguments": create_gql_mutation_delete_arguments_class(sa_model_class),
        # The return value of the update mutation query (the same object)
        deleted_count_arg_name: graphene.Field(graphene.Int),
        # this needs to exist or else the class cannot be created, it needs this item to exist.
        "mutate": lambda: None,
    }

    # Create Partial CreateObj Class
    delete_obj_partial_class = type(
        f"PartialDelete{cls_name}", (graphene.Mutation,), delete_obj_partial_class_items
    )

    # primary key name
    pk_name = inspect(sa_model_class).primary_key[0].name

    # Mutate Function Entry Point
    def mutate_func(root, info, **kwargs):
        # get pk_name from the outside scope
        nonlocal pk_name

        # Get the new instance data from kwargs[param_name]
        pk_id: dict = kwargs.get(pk_name)

        ###################
        # DELETE DATA
        ###################

        # Get DB Session
        s: Session = get_session_hook()

        # Find the object and flag it for deletion in the next commit.
        deleted_count = (
            s.query(sa_model_class)
            .filter(getattr(sa_model_class, pk_name) == int(pk_id))
            .delete()
        )

        # Commit (Delete)
        s.commit()

        # Return the ID of the deleted object as 'result'
        partial_delete_obj_class_invocation = {
            deleted_count_arg_name: int(deleted_count)
        }

        return delete_obj_partial_class(**partial_delete_obj_class_invocation)

    definitive_delete_func = mutate_func
    delete_hook = hooks.get(HookOperation.DELETE)
    if delete_hook:
        definitive_delete_func = delete_hook(mutate_func)

    # Create Definitive Class
    definitive_delete_obj_class_items = {"mutate": definitive_delete_func}

    definitive_create_obj_class = type(
        f"Delete{cls_name}",
        (delete_obj_partial_class,),
        definitive_delete_obj_class_items,
    )

    return definitive_create_obj_class
