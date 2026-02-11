def log_exceptions(log_message: str, context_arg_name: str| None = None):
    """
    Декоратор для обработки исключений с информативным логированием.
    context_arg_name (str): имя аргумента функции, который будет включён в лог при ошибке.
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Получаем значение аргумента для контекста
                print(log_message)
                context_value = None
                if context_arg_name in func.__code__.co_varnames:
                    arg_index = func.__code__.co_varnames.index(context_arg_name)
                    if arg_index < len(args):
                        context_value = args[arg_index]
                    else:
                        context_value = kwargs.get(context_arg_name)
                    print(f"Ошибка в {func.__name__} для {context_arg_name}={context_value}: {str(e)}")
                else:
                    print(f"Ошибка в {func.__name__}: {str(e)}")
                return None
        return wrapper
    return decorator


@log_exceptions("Ошибка при делении", "b")
def safe_divide(a, b):
    """Делит a на b, выбрасывает исключение при делении на ноль."""
    return a / b


def test_safe_divide_success():
    assert safe_divide(10, 2) == 5
    print("Тест успешного деления прошёл успешно!")

def test_safe_divide_zero_division(capsys=None):
    result = safe_divide(10, 0)
    assert result is None
    # Проверяем, что в выводе есть сообщение об ошибке
    if capsys:
        captured = capsys.readouterr()
        assert "Ошибка при делении" in captured.out
        assert "b=0" in captured.out
    print("Тест деления на ноль прошёл успешно!")

def test_safe_divide_negative():
    assert safe_divide(-10, 2) == -5
    print("Тест деления с отрицательным числом прошёл успешно!")

def main():
    test_safe_divide_success()
    test_safe_divide_zero_division()
    test_safe_divide_negative()
    print("Все тесты прошли успешно!")

if __name__ == "__main__":
    main()