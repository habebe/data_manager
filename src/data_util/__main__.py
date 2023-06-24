
import finsim.finsim_command as finsim_command
import sys

def main(args) -> int:
    cli = finsim_command.CLI([''] if len(args) == 1 else args[1:])
    return cli.command.run()

if __name__ == "__main__":
    sys.exit(main(sys.argv))

