def render(
    prefixes: list, element: list, href: str = "?id={curie}", db: str = None, depth: int = 0
) -> str:
    """Render hiccup-style HTML vector as HTML."""
    render_element = element.copy()
    indent = "  " * depth
    if not isinstance(render_element, list):
        raise Exception(f"Element is not a list: {element}")
    if len(render_element) == 0:
        raise Exception("Element is an empty list")
    tag = render_element.pop(0)
    if not isinstance(tag, str):
        raise Exception(f"Tag '{tag}' is not a string in '{element}'")
    output = f"{indent}<{tag}"

    if len(render_element) > 0 and isinstance(render_element[0], dict):
        attrs = render_element.pop(0)
        if tag == "a" and "href" not in attrs and "resource" in attrs:
            attrs["href"] = href.format(curie=attrs["resource"], db=db)
        for key, value in attrs.items():
            if key in ["checked"]:
                if value:
                    output += f" {key}"
            else:
                output += f' {key}="{value}"'

    if tag in ["meta", "link", "path"]:
        output += "/>"
        return output
    output += ">"
    spacing = ""
    if len(render_element) > 0:
        for child in render_element:
            if isinstance(child, str):
                output += child
            elif isinstance(child, list):
                output += "\n" + render(prefixes, child, href=href, db=db, depth=depth + 1)
                spacing = f"\n{indent}"
            else:
                raise Exception(f"Bad type for child '{child}' in '{element}'")
    output += f"{spacing}</{tag}>"
    return output


def render_text(element: list) -> str:
    """Render hiccup-style HTML vector as text."""
    if not isinstance(element, list):
        raise Exception(f"Element is not a list: {element}")
    if len(element) == 0:
        raise Exception("Element is an empty list")
    tag = element.pop(0)
    output = ""
    if len(element) > 0:
        for child in element:
            if isinstance(child, str):
                output += child
            elif isinstance(child, list):
                try:
                    output += render_text(child)
                except Exception as e:
                    raise Exception(f"Bad child in '{element}'", e)
            else:
                raise Exception(f"Bad type for child '{child}' in '{element}'")
    return output
