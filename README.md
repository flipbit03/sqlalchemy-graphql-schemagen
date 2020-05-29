# sqlalchemy-graphql-schemagen

Generate a full (query/mutation) GraphQL Schema from your defined classes in a declarative_meta() Model.


## Example

```py
#!/usr/bin/env python
from flask import Flask
from flask_graphql import GraphQLView
from sqlalchemy import create_engine
from medgraphqlapi import generate_custom_column_docstrings
from sqlalchemy_graphql_schemagen import SQLAlchemyGraphQLSchemaGenerator

from medgraphqlapi.database_schema import Base as sa_base_declarative

################################
# SQLAlchemy Stuff
################################
sa_connection_string = "mssql+pyodbc://XXXXXXXXXXXX"

# Create the Engine
sa_engine = create_engine(sa_connection_string)

# Generate more useful columns' docstrings (ex.: type length, collation, ...)
generate_custom_column_docstrings(sa_base_declarative)

################################
# Build a GraphQL Schema from the SQLAlchemy Schema, using sqlalchemy_graphql_schemagen!
################################
graphql_schema = SQLAlchemyGraphQLSchemaGenerator(
    "ApiName",
    sa_base_declarative,
    sa_connection_string=sa_connection_string,
).get_graphene_schema()

################################
# Instantiate Flask App
################################
app = Flask(__name__)

# Plug GraphQLView
app.add_url_rule("/graphql",
                 view_func=GraphQLView.as_view("graphql",
                                               schema=graphql_schema,
                                               graphiql=True))

# That's it!
if __name__ == "__main__":
    app.run(host="0.0.0.0")```