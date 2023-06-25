
from . import command
import sys

def main() -> int:
    args = sys.argv
    cli = command.CLI([''] if len(args) == 1 else args[1:])
    return cli.command.run()

if __name__ == "__main__":
    sys.exit(main())

