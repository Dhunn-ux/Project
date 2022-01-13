import pytest

from src.Project2 import (
    read_csv,
    days_of_flights,
    joints_flights_planes,
    joints_flights_airports,
    delay_agg,
    spark,
)


@pytest.fixture(scope="module")
def path():
    current_path = "C:/Users/Rev07/PycharmProjects/data2/test/file/"
    return current_path


@pytest.fixture(scope="module")
def flight(path):
    path1 = f"{path}flight.csv"
    df = spark.read.csv(path1, header=True, sep="\t")
    return df


def test_csv(path):
    # Arrange
    expected = 3
    # Act
    path1 = f"{path}flight.csv"
    result = read_csv(path1)

    # Assert
    assert result.count() == expected


def test_days_of_flights(flight):
    # Arrange
    expected = 2
    # Act
    total_days = days_of_flights(flight)

    # Assert
    assert total_days.collect()[0][0] == expected


def test_joints_flights_planes(flight, path):
    # Arrange
    expected = 72.0
    # Act
    path1 = f"{path}planes.csv"
    planes = spark.read.csv(path1, header=True, sep="\t")
    joins = joints_flights_planes(flight, planes)
    result = joins.agg({"day": "sum"})
    # Assert
    assert result.collect()[0][0] == expected


def test_delay_agg(flight, path):
    # Arrange
    expected = 1
    # Act
    path1 = f"{path}planes.csv"
    planes = spark.read.csv(path1, header=True, sep="\t")
    joining = joints_flights_planes(flight, planes)
    result = delay_agg(joining)
    # Assert
    assert result.count() == expected


def test_joints_flights_airports(flight, path):
    # Arrange
    expected = "Aberdeen"
    # Act
    path1 = f"{path}airport.csv"
    airports = spark.read.csv(path1, header=True, sep="\t")
    result = joints_flights_airports(flight, airports)
    # Assert
    assert result.collect()[0][0] == expected
