
from fastapi import FastAPI, Query, Response
from fastapi.responses import JSONResponse, PlainTextResponse
import duckdb
from typing import Optional

app = FastAPI(title="DuckLake OData-like API")

DB_PATH = "mart.duckdb"

def query_duckdb(sql: str, params: Optional[tuple] = None):
    con = duckdb.connect(DB_PATH, read_only=True)
    try:
        df = con.execute(sql, params or ()).fetchdf()
        return df.to_dict(orient="records")
    finally:
        con.close()

# Minimal OData $metadata endpoint (XML)
@app.get("/odata/$metadata")
def metadata():
    # This is a minimal static metadata doc for demo purposes
    xml = '''<?xml version="1.0" encoding="utf-8"?>
<edmx:Edmx Version="4.0" xmlns:edmx="http://docs.oasis-open.org/odata/ns/edmx">
  <edmx:DataServices>
    <Schema Namespace="DuckLake" xmlns="http://docs.oasis-open.org/odata/ns/edm">
      <EntityType Name="gold_page_count_all_time">
        <Key><PropertyRef Name="url"/></Key>
        <Property Name="url" Type="Edm.String" Nullable="false"/>
        <Property Name="views" Type="Edm.Int64" Nullable="true"/>
      </EntityType>
      <EntityContainer Name="Container">
        <EntitySet Name="gold_page_count_all_time" EntityType="DuckLake.gold_page_count_all_time"/>
      </EntityContainer>
    </Schema>
  </edmx:DataServices>
</edmx:Edmx>'''
    return Response(content=xml, media_type="application/xml")

# OData service root (lists entity sets)
@app.get("/odata/")
def service_root():
    # List available entity sets (tables)
    return JSONResponse(
        content={
            "@odata.context": "http://localhost:8000/odata/$metadata",
            "value": [
                {"name": "gold_page_count_all_time", "kind": "EntitySet", "url": "gold_page_count_all_time"}
            ]
        },
        media_type="application/json;odata.metadata=minimal"
    )

# OData entity set endpoint
@app.get("/odata/{table}")
def get_table(
    table: str,
    top: int = Query(100, alias="$top"),
    skip: int = Query(0, alias="$skip")
):
    sql = f"SELECT * FROM {table} LIMIT ? OFFSET ?"
    try:
        data = query_duckdb(sql, (int(top), int(skip)))
        return JSONResponse(
            content={
                "@odata.context": f"http://localhost:8000/odata/$metadata#/{table}",
                "value": data
            },
            media_type="application/json;odata.metadata=minimal"
        )
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": str(e)})
