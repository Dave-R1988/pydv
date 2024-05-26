from exceptions import UnknownEntityException

def generate_sql(entity):
    sql = []
    match(entity['EntityType']):
        case "Hub":
            return generate_sql_hub(entity)
        case "Link":
            return generate_sql_link(entity)
        case "Satellite":
            return ""
        case _: 
            raise UnknownEntityException(f"Unknown entity of type '{entity['EntityType']}' encountered.")

def generate_sql_hub(entity):
    entity_prefix = "Hub"
    bks_w_datatype = "\n, ".join([ f"{bk['Name']}BK                {bk['DataType'].upper()}        NOT NULL" for bk in entity['BusinessKeys'] ])

    sql = f"""
    IF OBJECT_ID(N'RAW.{entity_prefix}{entity['Name']}', N'U') IS NOT NULL  
        DROP TABLE RAW.{entity_prefix}{entity['Name']};  
    
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
    IF OBJECT_ID(N'RAW.{entity_prefix}{entity['Name']}', N'U') IS NOT NULL  
        DROP TABLE RAW.{entity_prefix}{entity['Name']}; 
    
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