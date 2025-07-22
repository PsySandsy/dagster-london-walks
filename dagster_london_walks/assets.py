"""
Software-defined assets of 3 of London's walking routes
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
from dagster_aws.s3 import S3Resource
from datetime import date
from pandas import concat, DataFrame, read_csv


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
    Show metadata about the London Loop in Dagster
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
    Show metadata about the Capital Ring in Dagster
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


@asset(group_name="green_chain")
def green_chain() -> DataFrame:
    """
    Manually create a dataframe showing the routes of The Green Chain Walk
    """

    d = {
        "section_name": [
            "Thamesmead to Lesnes Abbey",
            "Erith to Bostall Heath",
            "Bostall Heath to Oxleas Meadow",
            "Bostall Heath to Charlton Park",
            "Plumstead Common to Oxleas Meadow",
            "Thames Barrier to Oxleas Meadow",
            "Oxleas Wood to Mottingham Lane",
            "Shepherdleas Wood to Middle Park",
            "Mottingham Lane to Stumps Hill (Beckenham Place Park)",
            "Marvels Lane to Elmstead Wood",
            "Mottingham Lane to Beckenham Place Park",
            "Elmstead Wood to Chislehurst",
            "Stumps Hill (Beckenham Place Park) to Crystal Palace",
            "Nunhead to Crystal Palace",
            "Dulwich Park to Sydenham Hill Wood",
        ],
        "distance_miles": [
            2.75,
            3.5,
            3.8,
            3.6,
            1.8,
            4,
            3.9,
            4.3,
            4.8,
            0.9,
            6.2,
            1.7,
            3.7,
            5.9,
            1.3,
        ],
    }

    green_chain_df = DataFrame(data=d)

    return green_chain_df


@asset(group_name="green_chain")
def green_chain_sections(green_chain) -> MaterializeResult:
    """
    Show metadata about the Green Chain Walk in Dagster
    """

    return MaterializeResult(
        metadata={
            "Sections of the Green Chain": MetadataValue.md(green_chain.to_markdown()),
            "More Information": MetadataValue.url(
                "https://innerlondonramblers.org.uk/ideasforwalks/green-chain-walk-guides.html",
            ),
            "Map": MetadataValue.md(
                "![Pic](https://www.innerlondonramblers.org.uk/images/GreenChainWalk/GCW%20Sections%20v5%20-%20web.jpg)"
            ),
        }
    )


@asset(group_name="aws_integration")
def combine_all_walks(
    london_loop, capital_ring, green_chain, s3: S3Resource
):
    """
    Combine the dataframes of the London Loop, Capital Ring, and Green Chain Walk together.
    Then write this to S3 as a CSV with today's date as a prefix.
    """

    london_loop_with_walk_name = london_loop.assign(walk_name="London Loop")
    capital_ring_with_walk_name = capital_ring.assign(walk_name="Capital Ring")
    green_chain_with_walk_name = green_chain.assign(walk_name="Green Chain Walk")

    concatenated_df = concat(
        [
            london_loop_with_walk_name,
            capital_ring_with_walk_name,
            green_chain_with_walk_name,
        ],
        ignore_index=True,
    )

    s3_client = s3.get_client()

    s3_client.put_object(
        Bucket="david-dagster-input",
        Key=f"raw/{date.today()}_london-walks.csv",
        Body=concatenated_df.to_csv(index=False),
    )


@asset()
def distances(
    london_loop, capital_ring, green_chain
) -> MaterializeResult:
    """
    Show the total distance of the walks in Dagster
    """

    green_chain_distance = float(green_chain.distance_miles.sum())
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
                        {"Walk": "Green Chain Walk", "Distance": green_chain_distance}
                    ),
                    TableRecord(
                        {"Walk": "Capital Ring", "Distance": capital_ring_distance}
                    ),
                    TableRecord(
                        {"Walk": "London Loop", "Distance": london_loop_distance}
                    ),
                ],
            )
        }
    )


@asset(group_name="aws_integration")
def file_from_s3(s3: S3Resource) -> DataFrame:
    """
    Read today's london-walks.csv from S3 and materialise Metadata about it
    """

    s3_client = s3.get_client()

    s3_file = s3_client.get_object(
        Bucket="david-dagster-input", Key=f"raw/{date.today()}_london-walks.csv"
    )

    london_walks_df = read_csv(s3_file["Body"])

    return london_walks_df


@asset(group_name="aws_integration")
def metadata_of_s3_file(file_from_s3) -> MaterializeResult:
    data = file_from_s3

    return MaterializeResult(
        metadata={
            "Number of Sections": MetadataValue.int(len(data.section_name)),
            "Number of Sections per Walk": MetadataValue.md(
                data.walk_name.value_counts().to_markdown()
            ),
            "Preview of DataFrame": MetadataValue.md(data.head().to_markdown()),
        }
    )


@asset(group_name="aws_integration")
def write_transformation_to_s3(file_from_s3, s3: S3Resource) -> MaterializeResult:
    """
    Apply a Kilometer transformation to the walk data, then write that new df back to S3
    """

    data = file_from_s3

    data_with_km = data.assign(distance_km=round(data["distance_miles"] * 1.6, 2))

    s3_client = s3.get_client()

    s3_client.put_object(
        Bucket="david-dagster-input",
        Key=f"processed/{date.today()}_london-walks.csv",
        Body=data_with_km.to_csv(index=False),
    )

    return MaterializeResult(
        metadata={
            "Total Length in Miles": MetadataValue.float(
                round(float(data_with_km.distance_miles.sum()), 2)
            ),
            "Total Length in Kilometers": MetadataValue.float(
                round(float(data_with_km.distance_km.sum()), 2)
            ),
        }
    )
