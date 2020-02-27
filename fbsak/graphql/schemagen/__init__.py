from dataclasses import dataclass
from typing import List, Dict, Callable, Any, Type

from sqlalchemy import inspect
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.orm import Session, Query

import graphene
from graphene import ID, Argument
from graphene_sqlalchemy.converter import convert_sqlalchemy_composite
from graphene_sqlalchemy.registry import get_global_registry

from fbsak.baseclasses.logging import SimpleLoggableBase

from fbsak.graphql.schemagen.extra import create_or_get_graphql_filter_op_type_class
from fbsak.graphql.schemagen.hooks import SchemaGenHooksBase, HookDictType
from fbsak.graphql.schemagen.utilities import (
    get_graphql_field_type_for_sa_column,
    mask_ID_to_Int,
    get_columns_from_sa_model_class,
    sa_instance_to_dict,
    make_resolve_func_maker,
    gql_query_build_sa_obj_type,
    make_gql_object_description_from_sa_model_class,
    get_all_sa_model_classes,
    create_gql_update_input_object_type_from_sa_class,
    create_gql_mutation_update_arguments_class,
    create_gql_get_query_filter_input_object_type_from_sa_model,
    create_gql_get_query_order_by_filter_input_object_type_from_sa_model,
    create_create_obj_mutation_object,
    create_delete_obj_mutation_object,
    create_update_obj_mutation_object,
)


class SQLAlchemyGraphQLSchemaGenerator(SimpleLoggableBase):
    def __init__(
        self,
        api_name: str,
        declarative_base: DeclarativeMeta,
        get_session_func: Callable[[None], Session],
        op_hooks: HookDictType = None,
        sa_composite_converters: Dict[Any, Any] = None,
        graphene_schema_args: dict = None,
    ):
        self.graphene_schema_args = graphene_schema_args if graphene_schema_args else {}
        self.api_name = api_name
        self.declarative_base = declarative_base
        self.get_session_func = get_session_func

        # save the hooks
        self.op_hooks = op_hooks or {}

        # Save and register SQLAlchemy Composite Types
        self.sa_composite_converters = sa_composite_converters or tuple()
        self.register_composites()

    def register_composites(self):
        for (
            CompositeClass,
            CompositeConverterFunc,
        ) in self.sa_composite_converters.items():
            # Generate a Default Converter (STRING)
            def default_sa_composite_class_converter(composite, _registry):
                return graphene.String(description=composite.doc)

            # Register converter using the provided function or the default one will be provided.
            convert_sqlalchemy_composite.register(CompositeClass)(
                CompositeConverterFunc
                if callable(CompositeConverterFunc)
                else default_sa_composite_class_converter
            )

    ##################################################
    # Main Function - Entry Point
    ##################################################
    def get_graphene_schema(
        self, generate_query=True, generate_mutation=True, **graphene_schema_args
    ) -> graphene.Schema:

        schema_params = {"query": None, "mutation": None}
        schema_params.update(graphene_schema_args)

        if generate_query:
            self.l.debug("Generating Query schema...")
            schema_params["query"] = self.generate_query_schema()

        if generate_mutation:
            self.l.debug("Generating Mutation schema...")
            schema_params["mutation"] = self.generate_mutation_schema()

        self.l.debug("Assembling graphene.Schema from schema_params")

        schema = graphene.Schema(**schema_params)
        self.l.debug(
            f"""
---GRAPHQL SCHEMA START---
{str(schema)}
---GRAPHQL SCHEMA END---
"""
        )

        return graphene.Schema(**schema_params)

    ##################################################
    # Generate QUERY Schema
    ##################################################
    def generate_query_schema(self) -> type:

        # List of all Model Classes from SQLAlchemy
        sa_model_classes = get_all_sa_model_classes(self.declarative_base)

        # Root Query Class Attribute Dict
        root_query_class_dict = {"__doc__": f'Root Query Class for "{self.api_name}"'}

        # Iterate through all SQLAlchemy's classes, building GraphQL Objects
        for sa_model_class in sa_model_classes:
            # Get the actual table name for this entity.
            sa_table_name = sa_model_class.__tablename__

            self.l.debug(f"[Query] SQLAlchemy Class --> {sa_model_class.__name__}")

            # Get resolve_<object> Function
            resolve_func = make_resolve_func_maker(
                sa_model_class, self.get_session_func, self.op_hooks
            )

            # Attach resolve func to class.
            root_query_class_dict[resolve_func.__name__] = resolve_func

            # Build {Class} = graphene.List({Class})
            graphql_model_class = gql_query_build_sa_obj_type(sa_model_class)

            # Attach {Class} = graphene.List({Class})
            root_query_class_dict[f"{sa_table_name}"] = graphene.List(
                graphql_model_class,
                description=make_gql_object_description_from_sa_model_class(
                    sa_model_class
                ),
                filters=create_gql_get_query_filter_input_object_type_from_sa_model(
                    sa_model_class
                ),
                order_by=create_gql_get_query_order_by_filter_input_object_type_from_sa_model(
                    sa_model_class
                ),
                page=Argument(
                    graphene.Int,
                    default_value=1,
                    description="(Pagination) Page Number",
                ),
                perpage=Argument(
                    graphene.Int,
                    default_value=50,
                    description="(Pagination) Results Per Page",
                ),
            )

        # Build Main Class and attach objects built in the last step.
        root_query_class = type(
            f"Query_{self.api_name}", (graphene.ObjectType,), root_query_class_dict
        )

        # Return the newly built class.
        return root_query_class

    ##################################################
    # Generate Mutation Schema
    ##################################################
    def generate_mutation_schema(self) -> type:

        root_mutation_class_dict = {
            "__doc__": f'Root Mutation Class for "{self.api_name}"'
        }
        for sa_model_class in get_all_sa_model_classes(self.declarative_base):
            class_name = sa_model_class.__name__
            self.l.debug(f"[Mutation] SQLAlchemy Class --> {class_name}")

            graphene.Int()

            ################################
            # UPDATE
            ################################
            update_obj_class = create_update_obj_mutation_object(
                sa_model_class, self.get_session_func, self.op_hooks
            )

            root_mutation_class_dict[
                f"update_{class_name.lower()}"
            ] = update_obj_class.Field()

            ################################
            # CREATE
            ################################
            create_obj_class = create_create_obj_mutation_object(
                sa_model_class, self.get_session_func, self.op_hooks
            )

            root_mutation_class_dict[
                f"create_{class_name.lower()}"
            ] = create_obj_class.Field()

            ################################
            # DELETE
            ################################
            delete_obj_class = create_delete_obj_mutation_object(
                sa_model_class, self.get_session_func, self.op_hooks
            )

            root_mutation_class_dict[
                f"delete_{class_name.lower()}"
            ] = delete_obj_class.Field()

        root_mutation_class = type(
            f"Mutation_{self.api_name}",
            (graphene.ObjectType,),
            root_mutation_class_dict,
        )

        return root_mutation_class
