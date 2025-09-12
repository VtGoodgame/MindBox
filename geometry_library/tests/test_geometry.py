"""
Тесты для модуля фигур (Triangle и Circle)
Используется pytest для автоматической проверки функций.
"""

import pytest
from math import pi, sqrt, isclose
from geometry_library.square import Triangle, Circle  


@pytest.mark.describe("Площадь и периметр треугольников")
def test_triangle_equilateral():
    """
    Равносторонний треугольник:
    Проверяем площадь по формуле (sqrt(3)/4)*a^2 и периметр.
    """
    t = Triangle(3, 3, 3)
    expected_area = (sqrt(3) * 3**2) / 4
    assert isclose(t.area(), expected_area)
    assert t.perimeter() == 9
    assert not t.is_right_triangle()

def test_triangle_isosceles():
    """
    Равнобедренный треугольник:
    Проверяем площадь с использованием формулы Герона.
    """
    t = Triangle(5, 5, 8)
    p = (5 + 5 + 8) / 2
    expected_area = sqrt(p * (p - 5) * (p - 5) * (p - 8))
    assert isclose(t.area(), expected_area)
    assert t.perimeter() == 18
    assert not t.is_right_triangle()

def test_triangle_right():
    """
    Прямоугольный треугольник:
    Проверяем, что площадь считается как (a*b)/2 и метод is_right_triangle возвращает True.
    """
    t = Triangle(3, 4, 5)
    assert isclose(t.area(), 6)
    assert t.perimeter() == 12
    assert t.is_right_triangle()

def test_triangle_scalene():
    """
    Разносторонний треугольник:
    Проверка формулы Герона для всех сторон различной длины.
    """
    t = Triangle(4, 5, 6)
    p = (4 + 5 + 6) / 2
    expected_area = sqrt(p * (p - 4) * (p - 5) * (p - 6))
    assert isclose(t.area(), expected_area)
    assert t.perimeter() == 15
    assert not t.is_right_triangle()

def test_triangle_invalid_sides():
    """
    Проверка обработки некорректных данных:
    отрицательные стороны и нарушение неравенства треугольника.
    """
    with pytest.raises(ValueError):
        Triangle(-1, 2, 3)
    with pytest.raises(ValueError):
        Triangle(1, 2, 3)  


@pytest.mark.describe("Площадь и периметр круга")
def test_circle_by_radius():
    """
    Круг по радиусу:
    Проверка площади и периметра.
    """
    c = Circle(radius=3)
    assert isclose(c.area(), pi * 3**2)
    assert isclose(c.perimeter(), 2 * pi * 3)

def test_circle_by_diameter():
    """
    Круг по диаметру:
    Проверка преобразования диаметра в радиус и расчета площади и периметра.
    """
    c = Circle(diameter=10)
    assert isclose(c.area(), pi * 5**2)
    assert isclose(c.perimeter(), 2 * pi * 5)

def test_circle_invalid():
    """
    Проверка обработки некорректных данных:
    отсутствие радиуса и диаметра, отрицательные значения.
    """
    with pytest.raises(ValueError):
        Circle()
    with pytest.raises(ValueError):
        Circle(radius=-1)
    with pytest.raises(ValueError):
        Circle(diameter=0)
