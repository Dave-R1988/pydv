from exceptions import UnknownEntityException

def entity_order():
    return ["Hub", "Link", "Satellite"]

def generate_sql(entity):
    match(entity['EntityType']):
        case "Hub":
            return generate_sql_hub(entity)
        case "Link":
            return generate_sql_link(entity)
        case "Satellite":
            return generate_sql_satellite(entity)
        case _: 
            raise UnknownEntityException(f"Unknown entity of type '{entity['EntityType']}' encountered.")

def generate_sql_hub(entity):
    entity_prefix = "Hub"
    bks_w_datatype = "\n, ".join([ f"{bk['Name']}BK                {bk['DataType'].upper()}        NOT NULL" for bk in entity['BusinessKeys'] ])

    sql = f"""
    CREATE TABLE RAW.{entity_prefix}{entity['Name']}
    (
          {entity['Name']}HKey          BINARY(16)      NOT NULL
        , LoadDate                  DATETIME    NOT NULL
        , AuditId                   BIGINT      NOT NULL
        , SourceSystem              VARCHAR(30) NOT NULL
        , {bks_w_datatype}
        , CONSTRAINT PK_{entity_prefix}{entity['Name']} PRIMARY KEY NONCLUSTERED ({entity['Name']}HKey ASC) ON FG_RawDataVault_Index
    )
    ON FG_RawDataVault_Data;

    ALTER TABLE RAW.{entity_prefix}{entity['Name']}
    ADD
        CONSTRAINT DFLT_{entity_prefix}{entity['Name']}_LoadDate DEFAULT ('9999-12-31') FOR LoadDate
    """
    return sql

def generate_sql_link(entity):
    entity_prefix = "Lnk"
    hubs_w_datatype = [ f"{hub['HubName']}HKey          BINARY(16)          NOT NULL" for hub in entity['Hubs'] ]
    fks = "\n".join([
        f"""
        ALTER TABLE RAW.{entity_prefix}{entity['Name']}
            ADD CONSTRAINT FK_{entity_prefix}{entity['Name']}_Hub{hub['HubName']} FOREIGN KEY ({hub['HubName']}HKey)
                REFERENCES RAW.Hub{hub['HubName']} ({hub['HubName']}HKey);
        """
        for hub in entity['Hubs']
    ])
    
    sql = f"""
    CREATE TABLE RAW.{entity_prefix}{entity['Name']}
    (
          {entity['Name']}HKey          BINARY(16)      NOT NULL
        , LoadDate                  DATETIME    NOT NULL
        , AuditId                   BIGINT      NOT NULL
        , SourceSystem              VARCHAR(30) NOT NULL
        , {", ".join(hubs_w_datatype)}
        , CONSTRAINT PK_{entity_prefix}{entity['Name']} PRIMARY KEY NONCLUSTERED ({entity['Name']}HKey ASC) ON FG_RawDataVault_Index
    )
    ON FG_RawDataVault_Data;

    ALTER TABLE RAW.{entity_prefix}{entity['Name']}
    ADD
        CONSTRAINT DFLT_{entity_prefix}{entity['Name']}_LoadDate DEFAULT ('9999-12-31') FOR LoadDate;

    {fks}
    """
    return sql

def generate_sql_satellite(entity):
    entity_prefix = "HSat" if entity['ParentEntityType'] == "Hub" else "LSat"
    parent_w_datatype = f"{entity['ParentEntityName']}HKey              BINARY(16)          NOT NULL"
    parent_prefix = "Hub" if entity['ParentEntityType'] == "Hub" else "Lnk"
    fk = f"""
        ALTER TABLE RAW.{entity_prefix}{entity['MiddleName']}{entity['Name']}
            ADD CONSTRAINT FK_{entity_prefix}{entity['MiddleName']}{entity['Name']}_{parent_prefix}{entity['ParentEntityName']} FOREIGN KEY ({entity['ParentEntityName']}HKey)
                REFERENCES RAW.{parent_prefix}{entity['ParentEntityName']} ({entity['ParentEntityName']}HKey);
        """
    
    bus_cols = "\n".join([
        f"""
            , {bus_col['Name']}             {bus_col['DataType'].upper()}              NULL
        """
        for bus_col in entity['BusinessColumns']
    ])

    sql = f"""    
    CREATE TABLE RAW.{entity_prefix}{entity['MiddleName']}{entity['Name']}
    (
          {parent_w_datatype}
        , LoadDate                  DATETIME    NOT NULL
        , LoadEndDate               DATETIME    NOT NULL
        , AuditId                   BIGINT      NOT NULL
        , ExtractionDate            DATETIME    NOT NULL
        , {entity['Name']}HDiff         BINARY(16)  NOT NULL
        , SourceSystem              VARCHAR(30) NOT NULL
        {bus_cols}
        , CONSTRAINT PK_{entity_prefix}{entity['MiddleName']}{entity['Name']} PRIMARY KEY NONCLUSTERED ({entity['Name']}HKey ASC, LoadDate ASC) ON FG_RawDataVault_Index
    )
    ON FG_RawDataVault_Data;

    ALTER TABLE RAW.{entity_prefix}{entity['MiddleName']}{entity['Name']}
    ADD
        CONSTRAINT DFLT_{entity_prefix}{entity['MiddleName']}{entity['Name']}_LoadEndDate DEFAULT ('9999-12-31') FOR LoadEndDate;

    {fk}
    """
    return sql

def generate_sql_drop_if_exists(entity):
    match(entity['EntityType']):
        case "Hub":
            entity_prefix = "Hub"
        case "Link":
            entity_prefix = "Lnk"
        case "Satellite":
            entity_prefix = f"HSat{entity['MiddleName']}" if entity['ParentEntityType'] == "Hub" else f"LSat{entity['MiddleName']}"
        case _: 
            raise UnknownEntityException(f"Unknown entity of type '{entity['EntityType']}' encountered.")
        
    table_name = f"{entity_prefix}{entity['Name']}"
    sql = f"""
    IF OBJECT_ID(N'RAW.{table_name}') IS NOT NULL
        DROP TABLE RAW.{table_name};
    """
    return sql