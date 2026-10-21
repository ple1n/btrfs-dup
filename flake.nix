{
  description = "Btrfs deduplication scanner";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = { self, nixpkgs, rust-overlay }:
    let
      systems = [ "x86_64-linux" "aarch64-linux" ];
      forAllSystems = nixpkgs.lib.genAttrs systems;
      forSystem = system:
        let
          pkgs = import nixpkgs {
            inherit system;
            overlays = [ rust-overlay.overlays.default ];
          };
          rust = pkgs.rust-bin.stable."1.91.0".default;
          rustPlatform = pkgs.makeRustPlatform {
            cargo = rust;
            rustc = rust;
          };
        in
        { inherit pkgs rust rustPlatform; };
    in
    {
      packages = forAllSystems (system:
        let
          inherit (forSystem system) pkgs rustPlatform;
        in
        {
          default = rustPlatform.buildRustPackage {
            pname = "btrfs-dup";
            version = "0.1.0";
            src = self;
            cargoLock.lockFile = ./Cargo.lock;
            nativeBuildInputs = [
              pkgs.llvmPackages.clang
              pkgs.llvmPackages.libclang
              pkgs.pkg-config
            ];
            buildInputs = [
              pkgs.gtk3
              pkgs.libsoup_3
              pkgs.openssl
              pkgs.xdotool
              pkgs.webkitgtk_4_1
            ];
            LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
            LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [
              pkgs.llvmPackages.libclang.lib
              pkgs.llvmPackages.libllvm
              pkgs.gtk3
              pkgs.libsoup_3
              pkgs.openssl
              pkgs.xdotool
              pkgs.webkitgtk_4_1
            ];
          };
        });

      devShells = forAllSystems (system:
        let
          inherit (forSystem system) pkgs rust;
        in
        {
          default = pkgs.mkShell {
            packages = [
              rust
              pkgs.pkg-config
              pkgs.llvmPackages.clang
              pkgs.llvmPackages.libclang
              pkgs.llvmPackages.libllvm
              pkgs.gtk3
              pkgs.libsoup_3
              pkgs.openssl
              pkgs.xdotool
              pkgs.webkitgtk_4_1
            ];
            LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";
            LD_LIBRARY_PATH = pkgs.lib.makeLibraryPath [
              pkgs.llvmPackages.libclang.lib
              pkgs.llvmPackages.libllvm
              pkgs.gtk3
              pkgs.libsoup_3
              pkgs.webkitgtk_4_1
            ];
          };
        });
    };
}
