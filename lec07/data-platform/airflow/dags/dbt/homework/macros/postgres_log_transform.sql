{% macro postgres__log_transform(column, base, offset) %}
    {# PostgreSQL-specific implementation: use log10() for base 10, ln() for natural log #}
    {% if base == 10 %}
        case
            when {{ column }} is null or {{ column }} + {{ offset }} <= 0 then null
            else log10({{ column }} + {{ offset }})
        end
    {% else %}
        {# For other bases, use natural log and divide: log_b(x) = ln(x) / ln(b) #}
        case
            when {{ column }} is null or {{ column }} + {{ offset }} <= 0 then null
            else ln({{ column }} + {{ offset }}) / ln({{ base }})
        end
    {% endif %}
{% endmacro %}

