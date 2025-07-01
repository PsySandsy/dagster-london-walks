"""
Software-defined Assets of an Hello World variable
"""

from dagster import (
    asset,
    MetadataValue,
    MaterializeResult,
    TableColumn,
    TableMetadataValue,
    TableRecord,
    TableSchema,
)
from pandas import DataFrame


@asset(group_name="london_loop")
def london_loop() -> DataFrame:
    """
    Manually create a dataframe showing the routes of The London Loop
    """

    d = {
        "section_number": list(range(1, 25)),
        "section_name": [
            "Erith to Old Bexley",
            "Old Bexley to Petts Wood",
            "Petts Wood to West Wickham Common",
            "West Wickham Common to Hamsey Green",
            "Hamsey Green to Coulsdon South",
            "Coulsdon South to Banstead Downs",
            "Banstead Downs to Ewell",
            "Ewell to Kingston Bridge",
            "Kingston Bridge to Hatton Cross",
            "Hatton Cross to Hayes & Harlington",
            "Hayes & Harlington to Uxbridge",
            "Uxbridge to Harefield West",
            "Harefield West to Moor Park",
            "Moor Park to Hatch End",
            "Hatch End to Elstree",
            "Elstree to Cockfosters",
            "Cockfosters to Enfield Lock",
            "Enfield Lock to Chingford",
            "Chingford to Chigwell",
            "Chigwell to Havering-atte-Bower",
            "Havering-atte-Bower to Harold Wood",
            "Harold Wood to Upminster Bridge",
            "Upminster Bridge to Rainham",
            "Rainham to Purfleet",
        ],
        "distance_miles": [
            8.7,
            7.5,
            9.3,
            9.3,
            6.4,
            5,
            4.1,
            8,
            10,
            4,
            7.5,
            5.2,
            5.2,
            4.8,
            9.3,
            10.9,
            8.8,
            5,
            4.5,
            6.6,
            5,
            4.5,
            4.5,
            5.1,
        ],
    }

    london_loop_df = DataFrame(data=d)

    return london_loop_df


@asset(group_name="london_loop")
def london_loop_sections(london_loop) -> MaterializeResult:
    """
    Show the London Loop DataFrame in Dagster
    """

    return MaterializeResult(
        metadata={
            "Sections of the Loop": MetadataValue.md(london_loop.to_markdown()),
            "More Information": MetadataValue.url(
                "https://innerlondonramblers.org.uk/ideasforwalks/loop-guides.html",
            ),
            "Map": MetadataValue.md(
                "![Pic](https://innerlondonramblers.org.uk/images/RingandLoop/Loop%20Sections%20Overview%20web.jpg)"
            ),
        }
    )

@asset(group_name="capital_ring")
def capital_ring() -> DataFrame:
    """
    Manually create a dataframe showing the routes of The Capital Ring
    """

    d = {
        "section_name": [
            "Woolwich to Falconwood",
            "Falconwood to Grove Park",
            "Grove Park to Crystal Palace",
            "Crystal Palace to Streatham",
            "Streatham to Wimbledon Park",
            "Wimbledon Park to Richmond",
            "Richmond to Osterley Lock",
            "Osterley Lock to Greenford",
            "Greenford to South Kenton",
            "South Kenton to Hendon Park",
            "Hendon Park to Highgate",
            "Highgate to Stoke Newington",
            "Stoke Newington to Hackney Wick",
            "Hackney Wick to Beckton District Park",
            "Beckton District Park to Woolwich",
        ],
        "distance_miles": [
            7.2,
            4.4,
            7.8,
            4.4,
            5.7,
            7.3,
            4.8,
            5.5,
            5.6,
            7,
            5.6,
            5.6,
            4,
            5.2,
            4.4,
        ],
    }

    capital_ring_df = DataFrame(data=d)

    return capital_ring_df


@asset(group_name="capital_ring")
def capital_ring_sections(capital_ring) -> MaterializeResult:
    """
    Show the Capital Ring DataFrame in Dagster
    """

    return MaterializeResult(
        metadata={
            "Sections of the Ring": MetadataValue.md(capital_ring.to_markdown()),
            "More Information": MetadataValue.url(
                "https://innerlondonramblers.org.uk/ideasforwalks/capital-ring-guides.html",
            ),
            "Map": MetadataValue.md(
                "![Pic](https://innerlondonramblers.org.uk/images/RingandLoop/Capital-Ring-overview-web.jpg)"
            ),
        }
    )

@asset()
def distances(capital_ring, london_loop) -> MaterializeResult:
    """
    Show the total distance of the Capital Ring & London Loop in Dagster
    """

    capital_ring_distance = float(capital_ring.distance_miles.sum())
    london_loop_distance = float(london_loop.distance_miles.sum())

    return MaterializeResult(
        metadata={
            "Distances": TableMetadataValue(
                schema=TableSchema(
                    columns=[
                        TableColumn("Walk", "string", description="Name of the walk"),
                        TableColumn(
                            "Distance",
                            "float",
                            description="Distance of the walk in miles",
                        ),
                    ]
                ),
                records=[
                    TableRecord(
                        {"Walk": "Capital Ring", "Distance": capital_ring_distance}
                    ),
                    TableRecord(
                        {"Walk": "London Loop", "Distance": london_loop_distance}
                    )
                ],
            )
        }
    )
