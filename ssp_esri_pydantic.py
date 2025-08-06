import datetime
import logging
import os
import traceback
import uuid
from typing import Annotated, Any, Dict, Optional, Tuple, Union

import arcpy
from pydantic import (
    Base64Bytes,
    BaseModel,
    ConfigDict,
    Field,
    TypeAdapter,
    create_model,
)
from typing_extensions import Literal

logger = logging.getLogger()


class _Domain(BaseModel):
    """A class for storing information about domains from a feature service that mimics
    the structure of the domain object returned by arcpy.da.List_Domains"""

    codedValues: Optional[Dict[Any, str]] = None
    description: str
    domainType: str
    mergePolicy: str
    name: str
    owner: Optional[str] = None
    range: Tuple[Optional[Union[float]], Optional[Union[float]]] = (None, None)
    splitPolicy: str
    type: str


field_type_to_pydantic_dict = {
    "Blob": Base64Bytes,
    "Date": datetime.datetime,
    "Double": float,
    "Geometry": "GEOMETRY",  ## not validated
    "GlobalID": uuid.UUID,
    "Guid": uuid.UUID,
    "Integer": int,
    "SmallInteger": int,
    "Long": int,
    "Short": int,
    "OID": int,
    "Raster": Base64Bytes,
    "Single": float,
    "String": str,
    "Text": str,
    "Float": float,
}

field_type_min_max = {
    "SmallInteger": (-32_768, 32_767),
    "Short": (-32_768, 32_767),
    "SHORT": (-32_768, 32_767),
    "Integer": (-2_147_483_648, 2_147_483_647),
    "Long": (-2_147_483_648, 2_147_483_647),
    "LONG": (-2_147_483_648, 2_147_483_647),
    "Single": (-3.4e38, 1.2e38),
    "SINGLE": (-3.4e38, 1.2e38),
    "Float": (-3.4e38, 1.2e38),
    "FLOAT": (-3.4e38, 1.2e38),
    "Double": (-2.2e308, 1.8e308),
    "DOUBLE": (-2.2e308, 1.8e308),
    "BigInteger": (-9_007_199_254_740_991, 9_007_199_254_740_991),
    "BIGINTEGER": (-9_007_199_254_740_991, 9_007_199_254_740_991),
}


def make_domain_tuple(
    domains_list,
    domain_name: str,
    nullable: bool,
    insert_explicit: bool = True,
    max_length: int = None,
) -> tuple:
    the_domain = None
    for domain in domains_list:
        if domain.name == domain_name:
            the_domain = domain
            break
    if the_domain is None:
        logging.warning(f"Could not find domain named :{domain_name}")
        return None

    if the_domain.domainType == "CodedValue":
        domain_codes = list(the_domain.codedValues.keys())
        if len(domain_codes) == 0:
            return None
        if nullable is False and insert_explicit:
            if max_length is None:
                return (Literal[tuple(domain_codes)], Field())
            else:
                return (Literal[tuple(domain_codes)], Field(max_length=max_length))
        else:
            if max_length is None:
                fld = Field(default_factory=lambda: None)
                return (Optional[Literal[tuple(domain_codes)]], fld)
            else:
                fld = Field(default_factory=lambda: None, max_length=max_length)
                return (Optional[Literal[tuple(domain_codes)]], fld)
    else:
        # range domain
        min_val, max_val = the_domain.range
        pydantic_field_type = field_type_to_pydantic_dict[the_domain.type]
        if nullable is False and insert_explicit:
            fld = Field(ge=min_val, le=max_val)
            return (pydantic_field_type, fld)
        else:
            fld = Field(ge=min_val, le=max_val, default_factory=lambda: None)
            return (Optional[pydantic_field_type], fld)


class _NoExtras(BaseModel):
    model_config = ConfigDict(extra="forbid")


def make_table_adapter(table_path: str, *, insert_explicit: bool = True) -> TypeAdapter:
    """Makes a pydantic TypeAdapter based on the schema of a geodatabase table or
    feature class.  This type adapter can be used to validate data before sending it
    to the geodatabase.

    Parameters
    ----------
        table_path:
            The path to the the table or feature class to create the type adapter from.

        insert_explicit:
            If True, default values for fields will be ignored and non-nullable fields
            will all require values for the data to be considered valid.

            If False, non-nullable fields with default values in the geodatabase will
            allow input data to be considered valid even if the the values for the
            fields are null.

    Notes
    -----
    Geometry is not currently validated other than checking for a value.

    """
    try:
        table_name = os.path.basename(table_path)
        db_path = os.path.dirname(table_path)

        domains_list = arcpy.da.ListDomains(db_path)
        desc = arcpy.da.Describe(table_path)
        if "shapeFieldName" in desc:
            shape_field_name = desc["shapeFieldName"]
        else:
            shape_field_name = None

        if desc["subtypeFieldName"] != "":
            for field in [f.name for f in desc["fields"]]:
                if field.casefold() == desc["subtypeFieldName"].casefold():
                    subtype_field_name = field
                    break
            subtype_models = []
            subtypes_dict = arcpy.da.ListSubtypes(table_path)
            for subtype_code, subtype_dict in subtypes_dict.items():
                create_model_params = {}
                subtype_name = subtype_dict["Name"]
                model_name = f"{table_name}-{subtype_name}"

                if shape_field_name is not None:
                    fld = Field()
                    pydantic_tuple = (Any, fld)
                    create_model_params[shape_field_name] = pydantic_tuple
                    ## TODO validate shape

                st_fld_zip = zip(subtype_dict["FieldValues"].values(), desc["fields"])
                for st_tuple, field in st_fld_zip:
                    subtype_default, subtype_domain = st_tuple
                    pydantic_field_type = field_type_to_pydantic_dict[field.type]
                    if field.type == "String":
                        max_length = field.length
                    else:
                        max_length = None

                    if (
                        shape_field_name is not None
                        and field.name.casefold() == shape_field_name.casefold()
                    ):
                        # already added
                        continue

                    if field.editable is False:
                        ## non editable fields include stuff like objectids, globalids,
                        ##  and shape_length/area which are system maintained and we
                        ##  can/will never provide, skip these.
                        continue

                    if subtype_domain is not None:
                        domain_name = subtype_domain.name
                    elif field.domain != "":
                        domain_name = field.domain
                    else:
                        domain_name = None

                    pydantic_tuple = None
                    if domain_name is not None:
                        pydantic_tuple = make_domain_tuple(
                            domains_list,
                            domain_name,
                            field.isNullable,
                            insert_explicit,
                            max_length,
                        )
                    if pydantic_tuple is not None:
                        pass
                    elif field.name == subtype_field_name:
                        pydantic_tuple = (Literal[subtype_code], subtype_code)
                    else:
                        ge = None
                        le = None
                        if field.type in field_type_min_max:
                            ge, le = field_type_min_max[field.type]

                        if field.isNullable is False and insert_explicit:
                            fld = Field(max_length=max_length, ge=ge, le=le)
                            pydantic_tuple = (pydantic_field_type, fld)
                        else:
                            fld = Field(
                                default_factory=lambda: None,
                                max_length=max_length,
                                ge=ge,
                                le=le,
                            )
                            pydantic_tuple = (Optional[pydantic_field_type], fld)
                    create_model_params[field.name] = pydantic_tuple
                subtype_model = create_model(
                    model_name, **create_model_params, __base__=_NoExtras
                )
                subtype_models.append(subtype_model)
            validator_model = Annotated[
                Union[tuple(subtype_models)], Field(discriminator=subtype_field_name)
            ]
            table_model = TypeAdapter(validator_model)

        else:
            ## table is not subtyped
            create_model_params = {}
            model_name = f"{table_name}"
            if shape_field_name is not None:
                fld = Field()
                pydantic_tuple = (Any, fld)
                create_model_params[shape_field_name] = pydantic_tuple

            for field in desc["fields"]:
                pydantic_field_type = field_type_to_pydantic_dict[field.type]
                if field.type == "String":
                    max_length = field.length
                else:
                    max_length = None
                if (
                    shape_field_name is not None
                    and field.name.casefold() == shape_field_name.casefold()
                ):
                    # already added
                    continue

                if field.editable is False:
                    ## non editable fields include stuff like objectids, globalids,
                    ##  and shape_length/area which are system maintained and we
                    ##  can/will never provide, skip these.
                    continue

                if field.domain != "":
                    domain_name = field.domain
                else:
                    domain_name = None

                pydantic_tuple = None
                if domain_name is not None:
                    pydantic_tuple = make_domain_tuple(
                        domains_list,
                        domain_name,
                        field.isNullable,
                        insert_explicit,
                        max_length,
                    )

                if pydantic_tuple is not None:
                    pass
                else:
                    max_length = None
                    ge = None
                    le = None

                    if field.type == "String":
                        max_length = field.length
                    if field.type in field_type_min_max:
                        ge, le = field_type_min_max[field.type]

                    if field.isNullable is False and insert_explicit:
                        fld = Field(max_length=max_length, ge=ge, le=le)
                        pydantic_tuple = (pydantic_field_type, fld)
                    else:
                        fld = Field(
                            default_factory=lambda: None,
                            max_length=max_length,
                            ge=ge,
                            le=le,
                        )
                        pydantic_tuple = (Optional[pydantic_field_type], fld)
                create_model_params[field.name] = pydantic_tuple
            table_model = TypeAdapter(
                create_model(model_name, **create_model_params, __base__=_NoExtras)
            )
        return table_model
    except Exception as ex:
        tb = traceback.format_exc()
        logging.error(f"Exception in make_table_adapter: {ex=} {tb=}")
